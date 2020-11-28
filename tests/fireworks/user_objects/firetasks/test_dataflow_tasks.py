import logging

from imteksimfw.utils.logging import _log_nested_dict
from imteksimfw.utils.dict import compare
from imteksimfw.fireworks.user_objects.firetasks.dataflow_tasks import SearchDictTask

def test_search_dict_task_run():
    """Will lookup some dataset on the server."""
    logger = logging.getLogger(__name__)


    # TODO: dataset creation in test
    input = {
        "eb58eb70ebcddf630feeea28834f5256c207edfd": {
            "hash": "2f7d9c3e0cfd47e8fcab0c12447b2bf0",
            "relpath": "simple_text_file.txt",
            "size_in_bytes": 17,
            "utc_timestamp": 1606595093.53965
         },
        "eb58eb70ebcddf630feeea28834f5256c207edfe": {
            "hash": "2f7d9c3e0cfd47e8fcab0c12447b2bf1",
            "relpath": "another_simple_text_file.txt",
            "size_in_bytes": 17,
            "utc_timestamp": 1606595093.53965
        }
    }

    search = {"relpath": "simple_text_file.txt"}
    marker = {"relpath": True}

    task_spec = {
        'input': input,
        'search': search,
        'marker': marker,
        'stored_data': True,
    }

    t = SearchDictTask(**task_spec)
    fw_action = t.run_task({})

    logger.debug("FWAction:")
    _log_nested_dict(logger.debug, fw_action.as_dict())

    output = fw_action.stored_data['output']

    expected_response = ['eb58eb70ebcddf630feeea28834f5256c207edfd']
    assert compare(output, expected_response)