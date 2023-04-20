import time
from ensembl.core.data.connection import DBConnection
from ensembl.core.CoordSystemService import CoordSystemService
dbc = DBConnection('mysql+pymysql://ensro@mysql-ens-sta-1.ebi.ac.uk:4519/homo_sapiens_core_110_38')

with dbc.session_scope() as session:
    slice_name = 'chromosome:GRCh38:13:32315086:32400268:1' # forward only
    slice_name = 'chromosome:GRCh38:19:2269846:2270725' # mixed strand contigs, but FWD slice
    slice_name = 'chromosome:GRCh38:19:11455000:11474118' # REV slice
    coordSystemService = CoordSystemService(session)
    cs1 = coordSystemService.get_all()
    print (cs1)
#         cs2 = CoordSystemAdaptor.fetch_by_name(session, 'contig', None)
#         slice = SliceAdaptor.fetch_by_name(session, slice_name)
#         proj = SliceAdaptor.project(session, slice, cs2)

#         # ama = AssemblyMapperAdaptor(session, cs1, cs2)
#         # start = time.perf_counter()
#         # aaa = ama.fetch_assembly_mappings()
#         # print(f'First call: {time.perf_counter()-start}')
#         # start = time.perf_counter()
#         # aaa = ama.fetch_assembly_mappings()
#         # print(f'Second call: {time.perf_counter()-start}')

#         # [print(f'{k}:{v}') for k,v in aaa.items()]


