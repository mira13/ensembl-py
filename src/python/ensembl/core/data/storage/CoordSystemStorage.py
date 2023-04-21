# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session

from ensembl.core.data.model import CoordSystem as CoordSystemORM, Meta as MetaORM

from typing import Optional

from ensembl.ui_model.Assembly import CoordSystem

from functools import lru_cache

import warnings

__all__ = ['CoordSystemStorage']


class CoordSystemStorage():
    """Contains all the coordinate system related functions over CoordSystem ORM
    This storage allows the querying of information on the coordinate
    system table.
    CoordSystem table in most cases contains only one row, in worst case it
    will be about ten rows. So we should only fetch once all table content from
    database, and then on logic level cache it and using this in memory
    inctance perform search.
    Therefor starage needs only fetch-all and store-all methods
"""

    def __init__(self,
                 session: Session = None
                 ) -> None:
        if not session:
            raise ValueError(
                'Connection session is required for storage initilisation')
        self.__data_source = session

    def fetch_all(self, species_id: int = 1) -> list[CoordSystem]:
        """
        Arg [1]    : species_id: specifies particular species,
        usually one data_source has only one species.
        Example    : for cs in CoordSystemAdaptor.fetch_all(dbconnection):
                     print(f"{cs.name} {cs.version}";
        Description: Retrieves every coordinate system defined in the DB.
                     These will be returned in ascending order of species_id and rank. I.e.
                     The coordinate system with lower rank would be first in the
                     array.
        Returntype : List[ensembl.dbsql.CoordSystem]
        Caller     : CoordSystemService
        Status     : At Risk
                   : under development
        """

        rows = (self.__data_source.query(CoordSystemORM)
                .join(MetaORM, CoordSystemORM.species_id == MetaORM.species_id)
                .where(MetaORM.meta_key == 'species.production_name')
                .where(CoordSystemORM.species_id == species_id)
                .order_by(CoordSystemORM.rank)
                .all()
                )
        if (rows.count == 0):
            warnings.warn(f'Could not find any coordinate system', UserWarning)

        return rows

    def insert(self, coordSystemList):
        """
        Arg[1]     : list of CoordSystemORM to store in database
        Returntype : Error code, 0 if success
        Caller     : CoordSystemService
        Status     : To be implemented
        """

    def delete(self, coordSystemList):
        """
        Arg[1]     : list of CoordSystemORM to delete from database
        Returntype : Error code, 0 if success
        Caller     : CoordSystemService
        Status     : To be implemented
        """
