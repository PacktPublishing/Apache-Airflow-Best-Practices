from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook

def success_callback(context):
    slack_conn_id = 'slack'
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get('task_instance').log_url

    slack_msg = f"""
        :white_check_mark: Task has succeeded.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('execution_date')}
        <{log_url}| *Log URL*>
        """
    
    slack_alert = SlackWebhookOperator(
        task_id="slack-test",
        http_conn_id='slack_callbacks',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    
    return slack_alert.execute(context=context)