# Offline AI4S research controller

This standalone Python fixture exercises a research workflow without starting the SuperAgent service. The controller owns a SQLite task graph, bounded attempts and the result acceptance check. A local job can outlive its submitting process. A malformed synthetic input exercises recovery; the worker cannot change the acceptance criteria.

## Verify from the repository root on Linux

```sh
python3 -B -m unittest discover -v -s research -p test_controller.py
python3 -B -m unittest discover -v -s research/compute -p test_jobs.py
```

The two suites use temporary state and synthetic input. No model endpoint, dataset, remote queue or GitHub credentials are required. This fixture does not register a native Source, run ORAC, enforce provider billing limits, or train an atomic potential.
