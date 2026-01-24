# Recovery Plan

## Potential Issues
*   **Vertebra Availability:** If Vertebra packages are not installed, mock the import or instructions to build from source.
*   **Docker Connectivity:** Ensure host networking or proper port mapping for IPC vs TCP comparison.
*   **Noise:** Benchmarks on local dev machines can be noisy. Use CPU affinity or run multiple iterations.

## Fallbacks
*   If `aiomysql` is unstable, consider `asyncmy`.
*   If large result sets crash memory, reduce batch sizes but keep total row count high.
