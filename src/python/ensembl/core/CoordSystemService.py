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

from sqlalchemy.orm import Session
from ensembl.core.data.storage.CoordSystemStorage import CoordSystemStorage

from functools import lru_cache
from ensembl.ui_model.Assembly import CoordSystem
from typing import Optional

__all__ = ['CoordSystemService']


class CoordSystemService:
    """Representation of coordinate system logic.
    New species tend to have only one coordinate system - assembly (top level).
    For multi-coordinate-system species level is defined by rank.
    Note that many coordinate systems do a concept of a version
    for the entire coordinate system (though they may have a per-sequence
    version).  The 'chromosome' coordinate system usually has a version
    (i.e. the assembly version) but the clonal coordinate system does not
    (despite having individual sequence versions).  In the case where a
    coordinate system does not have a version an empty string ('') is used
    instead.
    Attributes:
        __data_source: keeps data source instance, used to retrive and store data
    Raises:
        Expection: when mapping to another level failed
        Status: to be tested
    """

    __type = 'coord_system_service'
    _mapping_paths = {}
    _top_level = None
    _seq_level = None
    _species_id = 1

    def __init__(self,
                 session: Session = None,
                 species_id: int = 1
                 ) -> None:
        if not session:
            raise ValueError(
                'Connection session is required, create it with DBConnection')
        self.__data_source = CoordSystemStorage(session)
        self._species_id = species_id
        self.get_all()

    @lru_cache(maxsize=2)
    def get_all(self) -> list[CoordSystem]:
        cs_raw_list = self.__data_source.fetch_all(self._species_id)
        result_list = []
        top_level = True
        _top_level = cs_raw_list[0]
        for cs_raw in cs_raw_list:
            if(cs_raw.attrib != None):
                seqlevel = (cs_raw.attrib.find("sequence_level") > 0)
                default = (cs_raw.attrib.find("default") > 0)
            cs = CoordSystem(cs_raw.name, cs_raw.version, cs_raw.rank, top_level,
                             seqlevel, default, cs_raw.species_id,
                             cs_raw.coord_system_id)
            if(seqlevel):
                _seq_level = cs
            result_list.append(cs)
            top_level = False

        return result_list

    def get_by_name(self,
                    name: str,
                    version: Optional[str] = None,
                    ) -> CoordSystem:
        """
        Arg [1]    : str name
                     The name of the coordinate system to retrieve.  Alternatively
                     this may be an alias for a real coordinate system.  Valid
                     aliases are 'toplevel' and 'seqlevel'.
        Arg [2]    : str version
                     The version of the coordinate system to retrieve.  If not
                     specified the default version will be used.
        Arg [3]    : int species_id (default: 1)
                     The species_id the coordinate system refers to.
                     If not specified the default value 1 will be used.
        Example    : cs_list = CoordSystemAdaptor.fetch_by_name('contig')
                     cs_list = CoordSystemAdaptor.fetch_by_name('chromosome','GRCh37')
        Description: Retrieves a coordinate system by its name
        Returntype : ensembl.dbsql.CoordSystem
        Exceptions : throw for wrong or missing arguments
        Caller     : general
        Status     : At Risk
                   : under development
        """
        warn_str = f'Could not find any coordinate system with name {name}'

        if name == 'seqlevel':
            return self._seq_level

        if name == 'toplevel':
            return self._top_level
