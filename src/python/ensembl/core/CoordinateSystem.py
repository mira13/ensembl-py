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

__all__ = ['CoordinateSystem']


class CoordinateSystem:
    """Representation of coordinate system logic.
    New species tend to have only one coordinate system - assembly (top level).
    For multi-coordinate-system species level is defined by rank.
    Note that many coordinate systems do not have a concept of a version
    for the entire coordinate system (though they may have a per-sequence
    version).  The 'chromosome' coordinate system usually has a version
    (i.e. the assembly version) but the clonal coordinate system does not
    (despite having individual sequence versions).  In the case where a
    coordinate system does not have a version an empty string ('') is used
    instead.
    Attributes:
        cache: define cache size, by default is 0
        session: keeps connection session_scope
    Raises:
        Expection: when mapping to another level failed
    """

    __type = 'coordinate_system'

    def __init__(self,
                 session: Session = None
                )->None:
        if not session: 
            raise ValueError('Connection session is required, create it with
                             DBConnection')
        self._session = session

    @classmethod
    def get_by_name(cls,
                      session: Session,
                      name: str,
                      version: Optional[str] = None,
                      species_id: int = 1
                      ) -> CoordSystem:
        """
        Arg [1]     : session: Session
                      The session object for connecting to the DB
        Arg [2]    : str name
                     The name of the coordinate system to retrieve.  Alternatively
                     this may be an alias for a real coordinate system.  Valid
                     aliases are 'toplevel' and 'seqlevel'.
        Arg [3]    : str version
                     The version of the coordinate system to retrieve.  If not
                     specified the default version will be used.
        Arg [4]    : int species_id (default: 1)
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
            # fetch sequence level
            return cls.fetch_sequence_level(session)

        if name == 'toplevel':
            # fetch top level
            return cls.fetch_top_level(session)

        if not version:
            stmt = (select(CoordSystemORM)
                    .where(
                    and_(
                        func.lower(CoordSystemORM.name) == name.lower(),
                        CoordSystemORM.attrib.like(r'%default%'),
                        CoordSystemORM.species_id == species_id
                    )
                    )
                    .order_by(CoordSystemORM.species_id, CoordSystemORM.rank)
                    )
        else:
            stmt = (select(CoordSystemORM)
                    .join(MetaORM, CoordSystemORM.species_id == MetaORM.species_id)
                    .where(
                and_(
                    func.lower(CoordSystemORM.name) == name.lower(),
                    func.lower(CoordSystemORM.version) == version.lower()
                )
            )
                .order_by(CoordSystemORM.species_id, CoordSystemORM.rank)
            )
            warn_str += f" and version {version}"

        cs_row = session.scalars(stmt).first()

        if not cs_row:
            warnings.warn(warn_str, UserWarning)
            return None

        toplevel = True if cs_row.name == 'top_level' else False
        seqlevel = False
        default = False
        if cs_row.attrib:
            if 'sequence_level' in cs_row.attrib:
                seqlevel = True
            if 'default' in cs_row.attrib:
                default = True

        return CoordSystem(cs_row.name, cs_row.version, cs_row.rank, toplevel, seqlevel, default, cs_row.species_id, cs_row.coord_system_id)
