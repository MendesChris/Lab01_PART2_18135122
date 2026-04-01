"""Great Expectations validation for the Raw (Bronze) layer."""

import os
import great_expectations as gx


DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Data")
GX_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "gx")


def run_validation(csv_path: str) -> bool:
    """Validate the raw CSV using Great Expectations.

    Creates a FileSystem-connected Data Context, defines 5+ expectations,
    runs validation, and generates Data Docs (HTML report).

    Returns True if validation passed, False otherwise.
    """
    print("[Validate] Running Great Expectations validation ...")

    context = gx.get_context(project_root_dir=GX_DIR)

    # --- Data Source ---
    data_source = context.data_sources.add_pandas_filesystem(
        name="raw_filesystem",
        base_directory=os.path.dirname(csv_path),
    )

    csv_asset = data_source.add_csv_asset(
        name="taxi_trips_csv",
        sep=",",
        header=True,
    )

    batch_definition = csv_asset.add_batch_definition_path(
        name="taxi_batch",
        path=os.path.basename(csv_path),
    )

    batch = batch_definition.get_batch()

    # --- Expectation Suite (5+ expectations) ---
    suite = context.suites.add(
        gx.ExpectationSuite(name="taxi_raw_suite")
    )

    # 1. pickup datetime must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="tpep_pickup_datetime"
        )
    )

    # 2. dropoff datetime must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="tpep_dropoff_datetime"
        )
    )

    # 3. trip_distance must be between 0 and 500 miles
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_distance",
            min_value=0,
            max_value=500,
        )
    )

    # 4. fare_amount should be between 0 and 1000
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="fare_amount",
            min_value=0,
            max_value=1000,
        )
    )

    # 5. passenger_count should be between 0 and 9
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=0,
            max_value=9,
        )
    )

    # 6. payment_type should be one of the known codes
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="payment_type",
            value_set=[0, 1, 2, 3, 4, 5, 6],
        )
    )

    # 7. total_amount must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="total_amount"
        )
    )

    # --- Validation Definition & Checkpoint ---
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="taxi_raw_validation",
            data=batch_definition,
            suite=suite,
        )
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="taxi_checkpoint",
            validation_definitions=[validation_definition],
            actions=[
                gx.checkpoint.UpdateDataDocsAction(
                    name="update_data_docs",
                ),
            ],
        )
    )

    result = checkpoint.run()
    passed = result.success

    print(f"[Validate] Validation {'PASSED' if passed else 'FAILED'}")
    print(f"[Validate] Data Docs generated at: {GX_DIR}/uncommitted/data_docs/")

    return passed


if __name__ == "__main__":
    csv = os.path.join(DATA_DIR, "raw", "taxi_trips.csv")
    run_validation(csv)
