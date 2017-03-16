# Copyright (c) 2016 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_config import cfg


CONF_BENCH = cfg.ConfigOpts()


def _register_opts():
    import os
    from os import path

    from workflow_parser import bench_drivers
    from workflow_parser import releases

    default_opts = [
        cfg.StrOpt("bench_driver",
                   choices=bench_drivers.get_driver_names(),
                   default=bench_drivers.DEFAULT_DRIVER),
    ]

    NOVA_PATCHER_GROUP = "nova_patcher"
    _nova_opts = [
        cfg.StrOpt("release",
                   choices=releases.Release.__members__.keys(),
                   default=releases.DEFAULT_RELEASE.name),
    ]

    CONF_BENCH.register_opts(default_opts)

    CONF_BENCH.register_group(cfg.OptGroup(name=NOVA_PATCHER_GROUP,
                                           title="Nova Patcher"))
    CONF_BENCH.register_opts(_nova_opts, group=NOVA_PATCHER_GROUP)

    file_path = path.dirname(os.path.realpath(__file__))
    DIR = path.join(file_path, "../bench.conf")
    CONF_BENCH(['--config-file', DIR], project="bench")


_register_opts()
