from datetime import datetime, timedelta
import logging

from google.cloud import datacatalog_lineage_v1 as lineage_v1
from google.cloud.datacatalog_lineage_v1 import EventLink, LineageEvent, Process

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


client = lineage_v1.LineageClient()


def _convert_to_proto_timestamp(timestamp):
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"


def create_linage_process(project_id, process_display_name):
    """
    Creates a lineage process.

    Args:
        project_id (str): The ID of the project.
        process_display_name (str): The display name of the process.

    Returns:
        str: The ID of the created process.
    """
    parent = f"projects/{project_id}/locations/europe-west1"
    process = Process()
    process.display_name = process_display_name
    process.attributes = {
        "owner": "ravishgarg@google.com",
        "framework": "PubSub to BQ Ingestion",
        "service": "Dataplex Custom Lineage"
    }

    try:
        response = client.create_process(parent=parent, process=process)
        logger.info(f"New linage process created: {response.name}")
        return response.name
    except Exception as e:
        logger.error(f"Failed to create lineage process: {e}")
        raise


def create_run(process_id, start_time, end_time, state, run_display_name):
    """
    Creates a lineage run.

    Args:
        process_id (str): The ID of the process.
        start_time (str): The start time of the run (format: %Y-%m-%dT%H:%M:%S.%fZ).
        end_time (str): The end time of the run (format: %Y-%m-%dT%H:%M:%S.%fZ).
        state (str): The state of the run.
        UNKNOWN = 0
        STARTED = 1
        COMPLETED = 2
        FAILED = 3
        ABORTED = 4
        run_display_name (str): The display name of the run.

    Returns:
        str: The ID of the created run.
    """
    run = lineage_v1.Run()
    run.start_time = start_time
    run.end_time = end_time
    run.state = state
    run.display_name = run_display_name
    run.attributes = {
        "owner": "RavishGarg",
        "purpose": "Dataplex Custom Lineage"
    }

    request = lineage_v1.CreateRunRequest(parent=process_id, run=run)

    try:
        response = client.create_run(request=request)
        logger.info(f"New run Created {response.name}")
        return response.name
    except Exception as e:
        logger.error(f"Failed to create lineage run: {e}")
        raise


def create_lineage_event(run_id, source_fqdn, target_fqdn, start_time, end_time):
    """
    Creates a lineage event.

    Args:
        run_id (str): The ID of the run.
        source_fqdn (str): The fully qualified name of the source entity.
        target_fqdn (str): The fully qualified name of the target entity.
        start_time (str): The start time of the event (format: %Y-%m-%dT%H:%M:%S.%fZ).
        end_time (str): The end time of the event (format: %Y-%m-%dT%H:%M:%S.%fZ).

    Returns:
        None
    """

    #TODO: add a logic to check prefix (path:, coracle:)
    source = lineage_v1.EntityReference()
    target = lineage_v1.EntityReference()
    source.fully_qualified_name = source_fqdn
    target.fully_qualified_name = target_fqdn
    links = [EventLink(source=source, target=target)]
    lineage_event = LineageEvent(links=links, start_time=start_time, end_time=end_time)

    request = lineage_v1.CreateLineageEventRequest(parent=run_id, lineage_event=lineage_event)

    try:
        response = client.create_lineage_event(request=request)
        logger.info("Lineage event created: %s", response.name)
    except Exception as e:
        logger.error(f"Failed to create lineage event: {e}")
        raise


def create_custom_linage_for_ingestion(project_id, process_display_name, source, target, start_time, end_time, state,
                                       run_display_name):
    """
    Creates a custom lineage for ingestion.

    Args:
        project_id (str): The ID of the project.
        process_display_name (str): The display name of the process.
        source (str): The fully qualified name of the source entity.
        target (str): The fully qualified name of the target entity.
        start_time (str): The start time of the run (format: %Y-%m-%dT%H:%M:%S.%fZ).
        end_time (str): The end time of the run (format: %Y-%m-%dT%H:%M:%S.%fZ).
        state (str): The state of the run.
        run_display_name (str): The display name of the run.

    Returns:
        None
    """
    process_id = create_linage_process(project_id, process_display_name=process_display_name)
    run_id = create_run(process_id=process_id, start_time=start_time, end_time=end_time, state=state,
                        run_display_name=run_display_name)
    create_lineage_event(run_id=run_id, start_time=start_time, end_time=end_time, source_fqdn=source,
                         target_fqdn=target)


def _get_process_id(project_id, process_display_name):
    """
    Gets the ID of a process with the given display name.

    Args:
        project_id (str): The ID of the project.
        process_display_name (str): The display name of the process.

    Returns:
        str: The ID of the process if found, None otherwise.
    """
    parent = f"projects/{project_id}/locations/europe-west1"
    processes = client.list_processes(parent=parent)
    for process in processes:
        if process.display_name == process_display_name:
            return process.name
    return None


def create_or_update_custom_linage_for_ingestion(project_id, process_display_name, source, target, start_time, end_time,
                                                 state, run_display_name):
    """
    Creates or updates a custom lineage for ingestion.

    Args:
        project_id (str): The ID of the project.
        process_display_name (str): The display name of the process. - Airflow DAG Name
        source (str): The fully qualified name of the source entity.
        https://cloud.google.com//data-catalog/docs/fully-qualified-names

        ## if path is provided it only shows file extension in linage UI (i.e. path:gs://raw-1234/test_schema/test.csv) else it shows file name (gs://raw-1234/test_schema/test.csv)

        target (str): The fully qualified name of the target entity.
        start_time (str): The start time of the run (format: %Y-%m-%dT%H:%M:%S.%fZ).
        end_time (str): The end time of the run (format: %Y-%m-%dT%H:%M:%S.%fZ).
        state (str): The state of the run.
        run_display_name (str): The display name of the run. - pas the task instance ID

    Returns:
        None
    """
    process_name = _get_process_id(project_id, process_display_name)
    if process_name:
        logger.info("Process %s already exists", process_name)
        run_id = create_run(process_id=process_name, start_time=start_time, end_time=end_time, state=state,
                            run_display_name=run_display_name)
        create_lineage_event(run_id=run_id, source_fqdn=source, target_fqdn=target, start_time=start_time,
                             end_time=end_time)
    else:
        logger.info("Creating new process, run, and lineage event")
        create_custom_linage_for_ingestion(project_id=project_id, process_display_name=process_display_name, source=source,
                                           target=target, start_time=start_time, end_time=end_time, state=state,
                                           run_display_name=run_display_name)


if __name__ == '__main__':
    # Configure logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    project_id = "raves-altostrat"
    process_display_name = "Dataplex_Custom_Lineage"
    dataset_id = "data_journey"
    table_id = "pubsub_direct"
    subscription_id = "dj_subscription_bq_direct"
    topic_name = "dj-pubsub-topic"
    source = "projects/raves-altostrat/topics/dj-pubsub-topic"
    target = "bigquery:raves-altostrat.data_journey.pubsub_direct"

    start_time = datetime.now() - timedelta(hours=3)
    process_start_time = _convert_to_proto_timestamp(start_time)  # Start time dag
    process_end_time = _convert_to_proto_timestamp(datetime.now())  # End Time

    state = "COMPLETED"
    run_display_name = "TASK_RUN_ID_2"
    create_or_update_custom_linage_for_ingestion(project_id, process_display_name, source, target, process_start_time,
                                                 process_end_time, state, run_display_name)