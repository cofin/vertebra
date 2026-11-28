"""Run an integer-addition WebAssembly export directly in the browser."""

__all__ = ("add_module", "app", "wasm_page")

# --8<-- [start:example]
from vertebra import App, get

ADD_WASM = bytes.fromhex("0061736d0100000001070160027f7f017f030201000707010361646400000a09010700200020016a0b")


@get("/add.wasm")
def add_module() -> tuple[int, list[tuple[str, str]], bytes]:
    """Serve the compiled two-input addition module."""
    return 200, [("content-type", "application/wasm")], ADD_WASM


@get("/")
def wasm_page() -> tuple[int, list[tuple[str, str]], bytes]:
    """Serve the WebAssembly calculator."""
    page = b"""<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WebAssembly calculator</title>
<style>
  body { font: 1rem system-ui; max-width: 32rem; margin: 3rem auto; padding: 0 1rem; }
  form, label { display: grid; gap: .5rem; }
  form { gap: 1rem; }
  input, button { font: inherit; padding: .5rem; }
</style>
<h1>WebAssembly calculator</h1>
<p>Add two signed 32-bit integers in the browser.</p>
<form>
  <label for="left">First integer
    <input id="left" type="number" min="-2147483648" max="2147483647"
           step="1" value="19" required>
  </label>
  <label for="right">Second integer
    <input id="right" type="number" min="-2147483648" max="2147483647"
           step="1" value="23" required>
  </label>
  <button type="submit" disabled>Add</button>
  <output for="left right" aria-live="polite">Loading WebAssembly</output>
</form>
<script type="module">
  const form = document.querySelector("form");
  const left = document.querySelector("#left");
  const right = document.querySelector("#right");
  const button = form.querySelector("button");
  const output = form.querySelector("output");

  try {
    const response = await fetch("/add.wasm");
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const { instance } = await WebAssembly.instantiateStreaming(response);

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const a = left.valueAsNumber;
      const b = right.valueAsNumber;
      output.textContent = `${a} + ${b} = ${instance.exports.add(a, b)}`;
    });
    output.textContent = "Ready";
    button.disabled = false;
  } catch (error) {
    output.textContent = `Could not load WebAssembly: ${error.message}`;
  }
</script>
</html>"""
    return 200, [("content-type", "text/html; charset=utf-8")], page


app = App(route_handlers=[wasm_page, add_module])

# --8<-- [end:example]
