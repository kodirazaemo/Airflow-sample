"""
Manual Alpha Vantage MCP read sample.

Trigger this DAG to:
1. Connect to the Alpha Vantage MCP server (HTTP / SSE / stdio)
2. Discover tools via tools/list
3. Call WTI (monthly) via tools/call

Configure ALPHA_VANTAGE_API_KEY in .env — never commit a real key.
Connection patterns: https://mcp.alphavantage.co/#connection-examples
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from alpha_vantage_mcp import AlphaVantageMcpClient, get_transport

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def list_alpha_vantage_mcp_tools(**context):
    transport = get_transport()
    if transport == 'rest':
        raise ValueError(
            'alpha_vantage_mcp_read requires ALPHA_VANTAGE_TRANSPORT=http, sse, or stdio '
            '(not rest). See https://mcp.alphavantage.co/#connection-examples'
        )
    with AlphaVantageMcpClient() as client:
        tools = client.list_tools()
    print(f'Alpha Vantage MCP transport={transport}; tools={len(tools)}')
    preview = tools[:25]
    print('Sample tools:', ', '.join(preview))
    context['ti'].xcom_push(key='mcp_tools', value=preview)
    context['ti'].xcom_push(key='mcp_tool_count', value=len(tools))
    return {'transport': transport, 'tool_count': len(tools), 'sample_tools': preview}


def read_wti_via_mcp(**context):
    with AlphaVantageMcpClient() as client:
        payload = client.call_tool('WTI', {'interval': 'monthly'})
    rows = payload.get('data') or []
    print(
        f"MCP tools/call WTI monthly returned {len(rows)} rows; "
        f"keys={sorted(payload)}"
    )
    if rows:
        print(f'Latest observation: {rows[0]}')
    context['ti'].xcom_push(key='wti_payload_keys', value=sorted(payload))
    context['ti'].xcom_push(key='wti_row_count', value=len(rows))
    # Keep XCom small — do not push the full series.
    latest = rows[0] if rows else None
    return {'function': 'WTI', 'interval': 'monthly', 'rows': len(rows), 'latest': latest}


with DAG(
    dag_id='alpha_vantage_mcp_read',
    default_args=default_args,
    description='Manual Alpha Vantage MCP tools/list + WTI tools/call',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['alpha-vantage', 'mcp', 'sample'],
) as dag:

    list_tools = PythonOperator(
        task_id='list_mcp_tools',
        python_callable=list_alpha_vantage_mcp_tools,
    )

    read_wti = PythonOperator(
        task_id='read_wti_monthly',
        python_callable=read_wti_via_mcp,
    )

    list_tools >> read_wti
