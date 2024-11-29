import datetime, json
 
from sklearn import datasets
import pandas as pd
 
from evidently.report import Report
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric
 
from evidently.test_suite import TestSuite
from evidently.test_preset import DataDriftTestPreset
from evidently import ColumnMapping
 
from evidently.ui.dashboards import CounterAgg
from evidently.ui.dashboards import DashboardPanelCounter
from evidently.ui.dashboards import DashboardPanelPlot
from evidently.ui.dashboards import PanelValue
from evidently.ui.dashboards import PlotType
from evidently.ui.dashboards import ReportFilter
from evidently.ui.dashboards import DashboardPanelTestSuite
from evidently.ui.dashboards import TestFilter
from evidently.ui.dashboards import TestSuitePanelType
from evidently.renderers.html_widgets import WidgetSize
from snowflake.snowpark.session import Session
 
from evidently.ui.workspace import Workspace
from evidently.ui.workspace import WorkspaceBase
from cryptography.hazmat.primitives import serialization
 
# bank_marketing = datasets.fetch_openml(name='bank-marketing', as_frame='auto')
# bank_marketing_data = bank_marketing.frame
 
# reference_data = bank_marketing_data[5000:7000]
# prod_simulation_data = bank_marketing_data[7000:]
# batch_size = 2000
 
WORKSPACE = "aps_data"
 
YOUR_PROJECT_NAME = "APS Model monitoring"
YOUR_PROJECT_DESCRIPTION = "APS scoring model monitoring dashboard"
 
def read_private_key(path):
    with open(path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
    return private_key
 
def get_snowflake_session():
    # Get the current credentials
    with open('snowflake_config.json') as f:
        connection_parameters = json.load(f)
    session = Session.builder.configs(connection_parameters).create()
    return session
 
def get_snowflake_service_session():
    private_key = read_private_key('private.pem')
    # Convert the key to bytes
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open('snowflake_service_config.json') as f:
        connection_parameters = json.load(f)
    connection_parameters['private_key'] = pkb
    session = Session.builder.configs(connection_parameters).create()
    return session
 
 
def convert_dtypes(df):
    for col in df.columns:
        if df[col].dtype in ['int8', 'int16']:
            df[col] = df[col].astype('int32')
        elif df[col].dtype in ['float32']:
            df[col] = df[col].astype('float64')
    return df
 
 
column_mapping = ColumnMapping()
column_mapping.target = 'BIND'
column_mapping.datetime = 'EFFECTIVEDATE'
column_mapping.id = 'SUBMISSIONNUMBER'
 
 
##----------------Get data-----------------------##
table_name = 'POC_ECI_TRANSFORM_DATA'
table_name2 = 'APS_TRAIN_DATA_SP_NONPROD_TEST1'
 
# session = get_snowflake_session()
session = get_snowflake_service_session()
raw_df = session.table(table_name).to_pandas()
 
raw_df['EFFECTIVEDATE'] = pd.to_datetime(raw_df['EFFECTIVEDATE'], unit='ns', errors='coerce')
raw_df['EFFECTIVEDATE'] = pd.to_datetime(raw_df['EFFECTIVEDATE'], unit='ns', errors='coerce')
 
raw_df = raw_df.drop(columns=['GOVERNINGCLASSCODE'])
raw_df = raw_df.sort_values(by='EFFECTIVEDATE')
split_date = '2023-01-01'
 
# Create train and test sets
reference_df = raw_df[raw_df['EFFECTIVEDATE'] < split_date]
current_df = raw_df[raw_df['EFFECTIVEDATE'] >= split_date]
reference_data = convert_dtypes(reference_df)
prod_simulation_data = convert_dtypes(current_df)
batch_size = 1000
 
time_stamp_start = datetime.datetime.strptime(split_date, '%Y-%m-%d')
 
 
 
def create_data_quality_report(i: int):
    report = Report(
        metrics=[
            DatasetDriftMetric(),
            ColumnDriftMetric(column_name="BIND"),
        ],
        timestamp=time_stamp_start + datetime.timedelta(days=i),
    )
 
    report.run(reference_data=reference_data, current_data=prod_simulation_data[i * batch_size : (i + 1) * batch_size], column_mapping=column_mapping)
    return report
 
def create_data_drift_test_suite(i: int):
    suite = TestSuite(
        tests=[
            DataDriftTestPreset()
        ],
        timestamp=time_stamp_start + datetime.timedelta(days=i),
        tags = []
    )
 
    suite.run(reference_data=reference_data, current_data=prod_simulation_data[i * batch_size : (i + 1) * batch_size], column_mapping=column_mapping)
    return suite
 
def create_project(workspace: WorkspaceBase):
    project = workspace.create_project(YOUR_PROJECT_NAME)
    project.description = YOUR_PROJECT_DESCRIPTION
    project.dashboard.add_panel(
        DashboardPanelCounter(
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            agg=CounterAgg.NONE,
            title="APS Dataset",
        )
    )
   
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title="Target Drift",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(
                    metric_id="ColumnDriftMetric",
                    metric_args={"column_name.name": "BIND"},
                    field_path=ColumnDriftMetric.fields.drift_score,
                    legend="target: BIND"
                ),
            ],
            plot_type=PlotType.LINE,
            size=WidgetSize.HALF
        )
    )
 
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title="Dataset Drift",
            filter=ReportFilter(metadata_values={}, tag_values=[]),
            values=[
                PanelValue(metric_id="DatasetDriftMetric", field_path="share_of_drifted_columns", legend="Drift Share"),
            ],
            plot_type=PlotType.BAR,
            size=WidgetSize.HALF
        )
    )
 
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="Data Drift tests",
            filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
            size=WidgetSize.HALF
        )
    )
 
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="Data Drift tests: detailed",
            filter=ReportFilter(metadata_values={}, tag_values=[], include_test_suites=True),
            size=WidgetSize.HALF,
            panel_type=TestSuitePanelType.DETAILED
 
        )
    )
 
    project.save()
    return project
 
 
def create_demo_project(workspace: str):
    ws = Workspace.create(workspace)
    project = create_project(ws)
 
    for i in range(0, 15):
        report = create_data_quality_report(i=i)
        ws.add_report(project.id, report)
 
        suite = create_data_drift_test_suite(i=i)
        ws.add_report(project.id, suite)
 
 
if __name__ == "__main__":
    create_demo_project(WORKSPACE)