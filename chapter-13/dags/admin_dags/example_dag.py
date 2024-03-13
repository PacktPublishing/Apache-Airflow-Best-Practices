
from datetime import datetime
import os

from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


from airflow import DAG
from airflow.models import DagBag
from airflow.auth.managers.fab.cli_commands.utils import get_application_builder
from airflow.security import permissions


# https://github.com/apache/airflow/blob/main/airflow/security/permissions.py
# Action Constants
READ = "can_read"
EDIT = "can_edit"
CREATE = "can_create"
DELETE = "can_delete"
MENU_ACCESS = "menu_access"

# Resource Constants
RESOURCE_ACTION = "Permissions"
RESOURCE_ADMIN_MENU = "Admin"
RESOURCE_AUDIT_LOG = "Audit Logs"
RESOURCE_BROWSE_MENU = "Browse"
RESOURCE_CONFIG = "Configurations"
RESOURCE_CONNECTION = "Connections"
RESOURCE_DAG = "DAGs"
RESOURCE_DAG_CODE = "DAG Code"
RESOURCE_DAG_DEPENDENCIES = "DAG Dependencies"
RESOURCE_DAG_PREFIX = "DAG:"
RESOURCE_DAG_RUN = "DAG Runs"
RESOURCE_DAG_WARNING = "DAG Warnings"
RESOURCE_CLUSTER_ACTIVITY = "Cluster Activity"
RESOURCE_DATASET = "Datasets"
RESOURCE_DOCS = "Documentation"
RESOURCE_DOCS_MENU = "Docs"
RESOURCE_IMPORT_ERROR = "ImportError"
RESOURCE_JOB = "Jobs"
RESOURCE_MY_PASSWORD = "My Password"
RESOURCE_MY_PROFILE = "My Profile"
RESOURCE_PASSWORD = "Passwords"
RESOURCE_PERMISSION = "Permission Views"  # Refers to a Perm <-> View mapping, not an MVC View.
RESOURCE_PLUGIN = "Plugins"
RESOURCE_POOL = "Pools"
RESOURCE_PROVIDER = "Providers"
RESOURCE_RESOURCE = "View Menus"
RESOURCE_ROLE = "Roles"
RESOURCE_SLA_MISS = "SLA Misses"
RESOURCE_TASK_INSTANCE = "Task Instances"
RESOURCE_TASK_LOG = "Task Logs"
RESOURCE_TASK_RESCHEDULE = "Task Reschedules"
RESOURCE_TRIGGER = "Triggers"
RESOURCE_USER = "Users"
RESOURCE_USER_STATS_CHART = "User Stats Chart"
RESOURCE_VARIABLE = "Variables"
RESOURCE_WEBSITE = "Website"
RESOURCE_XCOM = "XComs"




_add_users = """

airflow users create -e user_1.test.com -f user -l one -p user_1 -r Viewer -u user_1
airflow users create -e user_2.test.com -f user -l one -p user_2 -r Viewer -u user_2
"""

def _create_roles():

    for d in os.listdir("dags"):
        if d != "admin_dags":
            dag_bag = DagBag(os.path.join("dags",d))     
            
            with get_application_builder() as appbuilder:
                appbuilder.sm.add_role(d)
                role = appbuilder.sm.find_role(d)    
                perm: Permission | None = appbuilder.sm.create_permission(READ, "Website")
                appbuilder.sm.add_permission_to_role(role, perm)
                print(f"Added {perm} to role {d}")

                for r in ["Task Instances", "Website", "DAG Runs", "Audit Logs", "ImportError", "XComs", 
                "DAG Code", "Plugins", "My Password", "My Profile", "Jobs", "SLA Misses", "DAG Dependencies", "Task Logs"] :
                    perm: Permission | None = appbuilder.sm.create_permission(READ,r)
                    appbuilder.sm.add_permission_to_role(role, perm)
                
                for r in ["Task Instances", "My Password", "My Profile", "DAG Runs"]:
                    perm: Permission | None = appbuilder.sm.create_permission(EDIT,r)
                    appbuilder.sm.add_permission_to_role(role, perm)

                for r in ["DAG Runs", "Task Instances"]:
                    perm: Permission | None = appbuilder.sm.create_permission(CREATE,r)
                    appbuilder.sm.add_permission_to_role(role, perm)

                for r in ["DAG Runs", "Task Instances"]:
                    perm: Permission | None = appbuilder.sm.create_permission(DELETE,r)
                    appbuilder.sm.add_permission_to_role(role, perm)

                for r in ["View Menus", "Browse", "Docs", "Documentation", "SLA Misses", "Jobs", "DAG Runs", "Audit Logs", "Task Instances", "DAG Dependencies"]:
                    perm: Permission | None = appbuilder.sm.create_permission(MENU_ACCESS,r)
                    appbuilder.sm.add_permission_to_role(role, perm)

                for action in [READ,EDIT,CREATE,DELETE]:
                    for d_id in dag_bag.dags.keys():
                        perm: Permission | None = appbuilder.sm.create_permission(action, f"DAG:{d_id}")
                        appbuilder.sm.add_permission_to_role(role, perm)
                        print(f"Added {perm} to role {d}")

                user = appbuilder.sm.find_user(d)
                user.roles.append(role)
                appbuilder.sm.update_user(user)




with DAG(
    "admin_dags", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False
) as dag:

    create_users = BashOperator(task_id="add_users", bash_command=_add_users)
    create_roles = PythonOperator(task_id="create_roles",python_callable=_create_roles)




create_users >> create_roles