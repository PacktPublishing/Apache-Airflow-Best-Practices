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
"""Plugins example"""
from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from plugins.metrics_plugin.views.dashboard import MetricsDashboardView

# Creating a flask blueprint
metrics_blueprint = Blueprint(
    "Metrics",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


class MetricsPlugin(AirflowPlugin):
    """Defining the plugin class"""

    name = "Metrics Dashboard Plugin"
    flask_blueprints = [metrics_blueprint]
    appbuilder_views = [
        {"name": "Dashboard", "category": "Metrics", "view": MetricsDashboardView()}
    ]
