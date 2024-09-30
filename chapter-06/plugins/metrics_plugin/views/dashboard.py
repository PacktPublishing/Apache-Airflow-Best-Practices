#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Metrics Dashboard View"""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.auth.managers.models.resource_details import AccessView
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.auth import has_access_view
from flask_appbuilder import BaseView, expose
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class MetricsDashboardView(BaseView):
    """A Flask-AppBuilder View for a metrics dashboard"""

    default_view = "index"
    route_base = "/metrics_dashboard"

    @provide_session
    @expose("/")
    @has_access_view(AccessView.PLUGINS)
    def index(self, session: Session = NEW_SESSION):
        """Create dashboard view"""

        def interval(n: int):
            return f"now() - interval '{n} days'"

        dag_run_query = text(
            f"""
            SELECT
                dr.dag_id,
                SUM(CASE WHEN dr.state = 'success' AND dr.start_date > {interval(1)} THEN 1 ELSE 0 END) AS "1_day_success",
                SUM(CASE WHEN dr.state = 'failed' AND dr.start_date > {interval(1)} THEN 1 ELSE 0 END) AS "1_day_failed",
                SUM(CASE WHEN dr.state = 'success' AND dr.start_date > {interval(7)} THEN 1 ELSE 0 END) AS "7_days_success",
                SUM(CASE WHEN dr.state = 'failed' AND dr.start_date > {interval(7)} THEN 1 ELSE 0 END) AS "7_days_failed",
                SUM(CASE WHEN dr.state = 'success' AND dr.start_date > {interval(30)} THEN 1 ELSE 0 END) AS "30_days_success",
                SUM(CASE WHEN dr.state = 'failed' AND dr.start_date > {interval(30)} THEN 1 ELSE 0 END) AS "30_days_failed"
            FROM dag_run AS dr
            JOIN dag AS d ON dr.dag_id = d.dag_id
            WHERE d.is_paused != true
            GROUP BY dr.dag_id
        """
        )

        dag_run_stats = [dict(result) for result in session.execute(dag_run_query)]
        return self.render_template(
            "dashboard.html",
            title="Metrics Dashboard",
            dag_run_stats=dag_run_stats,
        )
