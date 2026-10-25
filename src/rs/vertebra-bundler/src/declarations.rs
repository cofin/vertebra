//! OXC AST-backed `.d.ts` isolated declarations emitter

use oxc_allocator::Allocator;
use oxc_ast::ast::{Declaration, Statement};
use oxc_parser::Parser;
use oxc_span::{GetSpan, SourceType, Span};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Fast `.d.ts` declaration generator powered by `oxc_parser` AST analysis.
#[pyclass(immutable_type)]
#[derive(Clone, Debug, Default)]
pub struct IsolatedDeclarationsGenerator {
    strip_internal: bool,
}

impl IsolatedDeclarationsGenerator {
    /// Create a new generator instance.
    #[must_use]
    pub const fn new(strip_internal: bool) -> Self {
        Self { strip_internal }
    }

    /// Generate TypeScript `.d.ts` declarations from a `.ts` or `.tsx` source string.
    pub fn emit_dts(&self, filename: &str, source_text: &str) -> Result<String, String> {
        let allocator = Allocator::default();
        let source_type = SourceType::from_path(filename).unwrap_or_else(|_| SourceType::ts());
        let parsed = Parser::new(&allocator, source_text, source_type).parse();
        if !parsed.diagnostics.is_empty() {
            return Err(format!(
                "TypeScript parse error in {filename}: {}",
                parsed.diagnostics[0]
            ));
        }

        let mut declarations = Vec::new();

        for stmt in &parsed.program.body {
            if let Statement::ExportDeclaration(export_decl) = stmt {
                match &export_decl.declaration {
                    Declaration::FunctionDeclaration(func) => {
                        let name = func
                            .id
                            .as_ref()
                            .map(|id| id.name.as_str())
                            .unwrap_or_default();

                        let mut params = Vec::new();
                        for item in &func.params.items {
                            let span = item.span();
                            let raw = &source_text[span.start as usize..span.end as usize];
                            let clean = raw.split_whitespace().collect::<Vec<_>>().join(" ");
                            params.push(clean);
                        }
                        if let Some(ref rest) = func.params.rest {
                            let span = rest.span();
                            let raw = &source_text[span.start as usize..span.end as usize];
                            let clean = raw.split_whitespace().collect::<Vec<_>>().join(" ");
                            params.push(clean);
                        }
                        let params_str = params.join(", ");

                        let return_type_str = if let Some(ref ret_type) = func.return_type {
                            let span = ret_type.span();
                            let raw = &source_text[span.start as usize..span.end as usize].trim();
                            if let Some(stripped) = raw.strip_prefix(':') {
                                format!(": {}", stripped.trim())
                            } else {
                                format!(": {raw}")
                            }
                        } else {
                            String::new()
                        };

                        declarations.push(format!(
                            "export declare function {name}({params_str}){return_type_str};"
                        ));
                    }
                    Declaration::TSInterfaceDeclaration(interface) => {
                        let name = interface.id.name.as_str();
                        let mut member_lines = Vec::new();
                        for member in &interface.body.body {
                            let span = member.span();
                            let member_raw = &source_text[span.start as usize..span.end as usize];

                            let prefix_start = (span.start as usize).saturating_sub(40);
                            let surrounding = &source_text[prefix_start..span.end as usize];
                            if self.strip_internal && surrounding.contains("@internal") {
                                continue;
                            }

                            let clean_member = member_raw.trim();
                            member_lines.push(format!("  {clean_member}"));
                        }
                        if member_lines.is_empty() {
                            declarations.push(format!("export interface {name} {{}}"));
                        } else {
                            let body = member_lines.join("\n");
                            declarations.push(format!("export interface {name} {{\n{body}\n}}"));
                        }
                    }
                    Declaration::TSTypeAliasDeclaration(_) => {
                        let span = export_decl.span;
                        let raw = &source_text[span.start as usize..span.end as usize];
                        declarations.push(raw.trim().to_string());
                    }
                    Declaration::VariableDeclaration(var_decl) => {
                        for decl in &var_decl.declarations {
                            let var_name = &source_text
                                [decl.id.span().start as usize..decl.id.span().end as usize];
                            if var_name.contains(':') {
                                declarations.push(format!("export declare const {var_name};"));
                            } else if let Some(ref init) = decl.init {
                                let type_hint = infer_type(source_text, init.span());
                                declarations
                                    .push(format!("export declare const {var_name}: {type_hint};"));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(declarations.join("\n"))
    }
}

fn infer_type(source: &str, span: Span) -> &'static str {
    let init_text = &source[span.start as usize..span.end as usize];
    if init_text.starts_with('"') || init_text.starts_with('\'') {
        "string"
    } else if init_text.parse::<f64>().is_ok() {
        "number"
    } else if init_text == "true" || init_text == "false" {
        "boolean"
    } else {
        "unknown"
    }
}

#[pymethods]
impl IsolatedDeclarationsGenerator {
    #[new]
    #[pyo3(signature = (strip_internal=true))]
    fn py_new(strip_internal: bool) -> Self {
        Self::new(strip_internal)
    }

    /// Emit `.d.ts` text for `source_text`.
    #[pyo3(name = "emit_dts")]
    fn py_emit_dts(&self, filename: &str, source_text: &str) -> PyResult<String> {
        self.emit_dts(filename, source_text)
            .map_err(PyValueError::new_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emit_dts_extracts_exported_signatures() {
        let generator = IsolatedDeclarationsGenerator::new(true);
        let src = r#"
export type UserId = string;
export function greet(name: string): string {
    return "Hello " + name;
}
export const VERSION: string = "1.0.0";
"#;
        let dts = generator.emit_dts("test.ts", src).unwrap();
        assert!(dts.contains("export type UserId = string;"));
        assert!(dts.contains("export declare function greet(name: string): string;"));
        assert!(dts.contains("export declare const VERSION: string;"));
    }

    #[test]
    fn test_emit_dts_multiline_function_and_interface() {
        let generator = IsolatedDeclarationsGenerator::new(true);
        let src = r#"
export function greet(
  name: string,
  count: number,
): string {
  return name.repeat(count);
}

export interface User {
  id: number;
  /** @internal */
  secret: string;
}
"#;
        let dts = generator.emit_dts("test.ts", src).unwrap();
        assert!(
            dts.contains("export declare function greet(name: string, count: number): string;")
        );
        assert!(dts.contains("export interface User"));
        assert!(dts.contains("id: number;"));
        assert!(!dts.contains("secret: string;"));
    }
}
