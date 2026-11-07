---
type: Research
title: Native Conditional Multipart Publication
description: Public object_store transport and credential seams, provider completion predicates, and bounded publication ownership.
status: draft
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
sources:
- id: sdk
  resource: src/rs/Cargo.lock::object_store-0.13.2
  title: Pinned public SDK source
- id: s3
  resource: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
  title: AWS conditional completion
- id: s3-copy
  resource: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
  title: AWS server-side part replay
- id: azure
  resource: https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list
  title: Azure conditional block commit
- id: gcs
  resource: https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite
  title: GCS generation-pinned rewrite
- id: gcs-preconditions
  resource: https://cloud.google.com/storage/docs/request-preconditions
  title: GCS generation preconditions
- id: oci
  resource: https://docs.oracle.com/en-us/iaas/tools/python/latest/api/object_storage/client/oci.object_storage.ObjectStorageClient.html#oci.object_storage.ObjectStorageClient.commit_multipart_upload
  title: OCI conditional commit
- id: locks
  resource: https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.lock
  title: Native file lock platform contracts
- id: s3-limits
  resource: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
  title: S3 minimum non-final multipart part size
- id: gcs-limits
  resource: https://cloud.google.com/storage/quotas#requests
  title: GCS minimum non-final multipart part size
tags:
- storage
- conditional
- multipart
- ownership
---

# Native Conditional Multipart Publication

<!-- truth: start -->
Atomic final publication requires the provider's destination precondition at the mutation itself. [^s3] [^azure] [^gcs] [^oci]
Public object_store transport and credential APIs provide the required wrapper seam without vendoring or auth reimplementation. [^sdk]
Completion failures retain abort/stage ownership; a checked HEAD never authorizes unconditional overwrite. [^sdk]
<!-- truth: end -->

## Grounded SDK Boundary

Pinned object_store 0.13.2 exports `client::{ReqwestConnector,HttpConnector,HttpClient,HttpRequest,HttpRequestBody}`. `HttpConnector::connect(&ClientOptions) -> Result<HttpClient>` and `HttpClient::execute(HttpRequest)` support private provider requests with the same resolved client configuration. `AmazonS3`, `MicrosoftAzure`, and `GoogleCloudStorage` expose `credentials().get_credential()`; public AwsAuthorizer and AzureAuthorizer perform SDK authorization. `MultipartStore` supplies create, put_part, complete and abort plus MultipartId/PartId. High-level multipart options cannot express final create/update predicates. [^sdk]

ObjectStoreEngine therefore retains `Arc<dyn ObjectStore>` for ordinary operations plus a private concrete publication backend and resolved transport/endpoint configuration. Memory uses real PutMode Create/Update without a second whole-object copy. Local publication uses same-filesystem staging and native OS locks covering all cooperating Vertebra mutations; physical parent identity unifies overlapping roots. Direct uncooperative filesystem writers are outside that guarantee. Native lock behavior and SDK-private staging require platform evidence. [^sdk] [^locks]

## Request Boundary and Runtime Activation

`CloudRequestClient::new(&ClientOptions)` constructs the public SDK connector, `execute(HttpRequest, operation)` performs real HTTP, and `execute_aws(HttpRequest, &AwsCredentialProvider, region, operation)` fetches and applies SDK credentials per invocation. `HttpClient::execute` returns non-2xx statuses as responses; provider wrappers own status, predicate and safe retry classification. SDK `HttpError::kind()` provides credential-free transport context; raw request/source-chain formatting and echoed server bodies are excluded from error output. [^sdk]

ch10:t02 owns the complete implementation and inline SDK/network tests under a temporary `#[cfg(test)] mod cloud_request;` declaration. ch12:t01 removes only that module gate when the first S3 initiation caller is wired, then runs ordinary library compilation and all-target Clippy. This stages real code without claiming runtime publication or retaining unused future provider APIs. Provider publication endpoint/refresh acceptance occurs in ch12:t01; Azure credential-form and SAS redaction acceptance occurs in ch13:t01–t02. [^sdk]

The SDK supports concrete credential injection through `AmazonS3Builder::with_credentials(AwsCredentialProvider)` and `MicrosoftAzureBuilder::with_credentials(AzureCredentialProvider)`. Providers implement public `CredentialProvider::get_credential() -> Result<Arc<Credential>>`; `AzureCredential::SASToken(Vec<(String,String)>)` and `StaticCredentialProvider::new` supply the real SAS fixture. Inline provider wrapper tests use those same concrete constructor paths and actual SDK authorizers with a local HTTP responder. No engine test hook, public Python config addition, mock HttpClient or custom auth is required. [^sdk]

## Root-Adjudicated Local Artifacts and Lock Identity

Use persistent `.vtb-cas-lock#0` inside each physical target parent directory. All cooperating Local mutations acquire the parent lock; two-directory operations deduplicate/order native physical directory IDs. This handles leaf case/Unicode aliases, overlapping roots and independently configured TMPDIR without a global temp registry. Payload staging occurs outside the lock; predicate, atomic link/rename and actual result metadata occur under lock. Persistent lockfiles are never unlinked/recreated. Preserve SDK-normal umask and existing-file read-lock compatibility; real permission errors remain visible.

Stages use same-parent `.vertebra-stage-<uuid>#<counter>`, with no '#' in the random base and a nonempty numeric suffix after the FIRST '#'. Pinned SDK local.rs::is_valid_file_path already rejects/ignores that grammar in both listing paths, so no legal SDK key is reserved. Appending to a legal caller basename containing '#nondigit' is unsafe and forbidden. Normal abort/cancellation removes only owned stages; no broad crash scavenger is added. Direct uncooperative OS writers remain outside the guarantee. [^sdk]

## Provider Protocol Findings

S3 completion accepts If-Match or If-None-Match `*`. An HTTP200 can contain an Error body and requires XML inspection. A 409 requires re-initiation and part replay; the root selects an honest typed 409 conflict with retained abort ownership and no hidden replay. A caller can start a fresh operation with rewindable input. Always staging/re-copying S3 would add unjustified I/O, latency and permissions. UploadPartCopy supports ranged server-side copying from an existing object. Client payload collection or re-reading an unrewindable producer is not valid recovery. [^s3] [^s3-copy]

Azure PutBlockList accepts conditional commit headers and object metadata. Its response supplies ETag. The public SDK block identifiers and credentials can be reused while privately issuing only the missing conditional commit request. Uncommitted block cleanup must respect Azure's actual lifecycle rather than deleting a competing committed blob. [^azure]

GCS JSON rewrite supports pinned source generation and destination ifGenerationMatch, including 0 for absence. Repeated requests carry rewriteToken until done; final resource contains the published object metadata. Ordinary ETag updates must resolve the current generation and use UpdateVersion with that generation. An arbitrary negative ETag requires predicate re-evaluation in a Create/Update CAS loop. [^gcs] [^gcs-preconditions]

OCI CommitMultipartUpload supports If-Match and If-None-Match `*`; arbitrary negative ETags must use current-ETag compare-and-commit, not unsupported arbitrary If-None-Match. The owned OCI transport retains signing and abort ownership. OCI uniform parts remain 8 MiB with an explicit 10000-part limit; this does not imply a 5 TiB supported upload promise. [^oci]

GCS XML and JSON ETags are not interchangeable. Existing public native tags remain XML ETags; generation fences translate predicates and publication receipts without cross-API string comparison. The pinned SDK `GetOptions { head: true, version: Some(generation), .. }` emits an XML HEAD with `generation` query and returns that generation's metadata. A successful JSON rewrite supplies the generation for this pinned receipt lookup; a successor must never be reported through an unfenced HEAD. Verify distinct XML/JSON values and a writer arriving immediately after rewrite. [^sdk] [^gcs-preconditions]

## Incremental Part Framing

Pinned SDK S3MultiPartUpload::put_part and GCSMultipartUpload::put_part forward each supplied PutPayload as one service part. Current MultipartUploadSession forwards every non-OCI producer chunk unchanged. Arbitrary 64 KiB chunks therefore violate the minimum 5 MiB non-final part requirement at completion. A bounded rolling frame must accumulate only the current part and await flush before consuming more producer data. The SDK WriteMultipart helper supplies buffering but finish consumes ownership; adopting it without preserving abort on failure would recreate the cleanup defect. [^s3-limits] [^gcs-limits]

## Source and Validation Bounds

The implementation is split into shared transport/lifecycle, platform-local primitives, and provider publication chapters. No Python/global process lock, whole-payload Vec/list/mmap spool, single-PUT size-cap fallback, SDK fork or handcrafted cloud auth is accepted. Literal percent key identity, actual final ETag/version, metadata, competing writes,200-with-error bodies, token continuation and cancellation cleanup are observable test requirements. Native success is separate from planning validation and from final whole-flow gates.

[^sdk]: Local installed `object_store-0.13.2/src/{client/http/connection.rs,client/http/body.rs,client/mod.rs,multipart.rs,aws/mod.rs,aws/builder.rs,aws/credential.rs,azure/mod.rs,azure/builder.rs,azure/credential.rs,gcp/mod.rs,gcp/client.rs,lib.rs}` and `src/rs/vertebra-storage/src/{store.rs,lib.rs,oci.rs}`.
[^s3]: [AWS CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html).
[^s3-copy]: [AWS UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html).
[^azure]: [Azure Put Block List](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list).
[^gcs]: [GCS objects.rewrite](https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite).
[^gcs-preconditions]: [GCS request preconditions](https://cloud.google.com/storage/docs/request-preconditions).
[^oci]: [OCI ObjectStorageClient commit_multipart_upload](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/object_storage/client/oci.object_storage.ObjectStorageClient.html#oci.object_storage.ObjectStorageClient.commit_multipart_upload).
[^locks]: [Rust File::lock](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.lock).

[^s3-limits]: [AWS multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html) — minimum 5 MiB for each non-final part.
[^gcs-limits]: [GCS XML API quotas and limits](https://cloud.google.com/storage/quotas#requests) — minimum 5 MiB non-final parts enforced on completion.
