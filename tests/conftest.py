# This file is part of dax_imgserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import pytest
from flask import Flask

from lsst.dax.imgserv import api_soda as is_api_soda


@pytest.fixture(scope="session")
def app():
    app = Flask(__name__)
    app.register_blueprint(is_api_soda.image_soda, url_prefix='/api/image/soda')

    with app.app_context():
        is_api_soda.load_imgserv_config(metaserv_url="mysql://NA@test.edu")

    @app.route('/test-endpoint')
    def test_endpoint():
        return 'got it', 200

    return app



