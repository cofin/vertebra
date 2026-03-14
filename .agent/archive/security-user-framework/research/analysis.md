# Research: Supervisor Import Error

## Issue
Tests are failing with `ImportError: cannot import name 'Supervisor' from 'vertebra._vertebra'`.

## Investigation
- **Python Code (`src/py/vertebra/supervisor.py`)**:
  ```python
  from vertebra._vertebra import Supervisor as NativeSupervisor
  ```
  This expects `vertebra._vertebra` to export a class named `Supervisor`.

- **Rust/PyO3 Code (`src/rs/vertebra-py/src/supervisor.rs`)**:
  ```rust
  #[pyclass(name = "NativeSupervisor")]
  pub struct NativeSupervisor { ... }
  ```
  This exports the class as `NativeSupervisor`.

## Conclusion
There is a naming mismatch. The Rust extension exposes `NativeSupervisor`, but the Python code tries to import `Supervisor`.

## Solution
Update `src/py/vertebra/supervisor.py` to import `NativeSupervisor` directly:
```python
from vertebra._vertebra import NativeSupervisor
```
