import datetime
import os.path
import time, json
import pandas as pd

from requests.exceptions import RequestException
from sklearn import datasets
from evidently.report import Report
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric

from evidently.collector.client import CollectorClient
from evidently.collector.config import CollectorConfig, IntervalTrigger, ReportConfig

from evidently.test_suite import TestSuite
from evidently.test_preset import DataDriftTestPreset

from evidently.ui.dashboards import DashboardPanelTestSuite, DashboardPanelPlot, PanelValue, PlotType
from evidently.ui.dashboards import ReportFilter
from evidently.ui.dashboards import TestFilter
from evidently.ui.dashboards import TestSuitePanelType
from evidently.renderers.html_widgets import WidgetSize
from evidently.ui.workspace import Workspace
from evidently import ColumnMapping
from snowflake.snowpark.session import Session


client = CollectorClient("http://localhost:8001")
COLLECTOR_ID = "default"
COLLECTOR_TEST_ID = " default_test"

WORKSPACE = "aps_data_online"

PROJECT_NAME = "APS Model monitoring - Live"
PROJECT_DESCRIPTION = "APS scoring model monitoring dashboard"

column_mapping = ColumnMapping()
column_mapping.target = 'BIND'
column_mapping.datetime = 'EFFECTIVEDATE'
column_mapping.id = 'SUBMISSIONNUMBER'

##----------------Util functions-------------------##
def get_snowflake_session():
    # Get the current credentials
    with open('aps_snowflake_code/snowflake_config.json') as f:
        connection_parameters = json.load(f)
    session = Session.builder.configs(connection_parameters).create()
    return session


def convert_dtypes(df):
    for col in df.columns:
        if df[col].dtype in ['int8', 'int16']:
            df[col] = df[col].astype('int32')
        elif df[col].dtype in ['float32']:
            df[col] = df[col].astype('float64')
    return df


##----------------Get data-----------------------##
table_name = 'ECI_TRANSFORM_DATA'

# session = get_snowflake_session()
# raw_df = session.table(table_name).to_pandas()

raw_file_path = 'complete_train_data.xlsx'
raw_df = pd.read_excel(raw_file_path)

raw_df['EFFECTIVEDATE'] = pd.to_datetime(raw_df['EFFECTIVEDATE'], unit='ns', errors='coerce')
raw_df['EFFECTIVEDATE'] = pd.to_datetime(raw_df['EFFECTIVEDATE'], unit='ns', errors='coerce')

raw_df = raw_df.drop(columns=['GOVERNINGCLASSCODE'])
raw_df = raw_df.sort_values(by='EFFECTIVEDATE')
split_date = '2023-01-01'

# Create train and test sets
reference_df = raw_df[raw_df['EFFECTIVEDATE'] < split_date][-5000:]
current_df = raw_df[raw_df['EFFECTIVEDATE'] >= split_date]
reference_data = convert_dtypes(reference_df)
prod_simulation_data = convert_dtypes(current_df)
mini_batch_size = 50

time_stamp_start = datetime.datetime.strptime(split_date, '%Y-%m-%d')


def setup_test_suite():
	suite = TestSuite(tests=[DataDriftTestPreset()], tags=[])
	suite.run(reference_data=reference_data, current_data=prod_simulation_data[:mini_batch_size])
	return ReportConfig.from_test_suite(suite)

def workspace_setup():
	ws = Workspace.create(WORKSPACE)
	project = ws.create_project(PROJECT_NAME)
	project.dashboard.add_panel(
		DashboardPanelTestSuite(
			title="Data Drift Tests",
			filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
			size=WidgetSize.HALF
		)
	)
	project.dashboard.add_panel(
		DashboardPanelTestSuite(
			title="Data Drift Tests",
			filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
			size=WidgetSize.HALF,
			panel_type=TestSuitePanelType.DETAILED
		)
	)
	project.save()

def setup_config():
	ws = Workspace.create(WORKSPACE)
	project = ws.search_project(PROJECT_NAME)[0]

	test_conf = CollectorConfig(trigger=IntervalTrigger(interval=10),
		report_config=setup_test_suite(), project_id=str(project.id))

	client.create_collector(COLLECTOR_TEST_ID, test_conf)
	client.set_reference(COLLECTOR_TEST_ID, reference_data)

def send_data():
	print("Start sending data")
	for i in range(50):
		try:
			data = prod_simulation_data[i * mini_batch_size : (i + 1) * mini_batch_size]
			client.send_data(COLLECTOR_TEST_ID, data)
			print("sent")
		except RequestException as e:
			print(f"collector service is not available: {e.__class__.__name__}")
		time.sleep(1)

def main():
	if not os.path.exists(WORKSPACE) or len(Workspace.create(WORKSPACE).search_project(PROJECT_NAME)) == 0:
		workspace_setup()

	setup_config()
	send_data()

if __name__ == '__main__':
	main()



