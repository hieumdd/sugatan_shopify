def assertion(res):
    results = res["results"]
    assert results["num_processed"] > 0
    assert results["output_rows"] > 0
    assert results["num_processed"] == results["output_rows"]
