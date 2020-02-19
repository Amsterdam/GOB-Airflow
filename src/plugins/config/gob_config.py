"""
GOB Configuration

All DAGs and properties of DAGs are read from the info in this module

Specific parameters for catalogues and collections can be added here, like:
- application (for gebieden imports)
- start time
- expected durations (to handle sensor timeouts)
- and whatever more

It is the intention to keep this configuration as readable as possible.
Any technical parameter should be given a human readable name and converted to a technical name in the code
Example: don't use a name like poke_interval but use a functional name like check_for_result
"""
CATALOGUES = {
    # 'brk': {
    #     'collections': {
    #         'kadastraleobjecten': {},
    #         'zakelijkerechten': {},
    #         'kadastralesubjecten': {},
    #         'tenaamstellingen': {},
    #         'aantekeningenrechten': {},
    #         'aantekeningenkadastraleobjecten': {},
    #         'stukdelen': {},
    #         'aardzakelijkerechten': {},
    #         'gemeentes': {},
    #         'meta': {}
    #     }
    # },
    # 'wkpb': {
    #     'collections': {
    #         'beperkingen': {},
    #         'dossiers': {},
    #         'brondocumenten': {}
    #     }
    # },
    'nap': {
        'collections': {
            'peilmerken': {}
        }
    },
    # 'meetbouten': {
    #     'collections': {
    #         'meetbouten': {},
    #         'metingen': {},
    #         'referentiepunten': {},
    #         'rollagen': {}
    #     }
    # },
    'gebieden': {
       'collections': {
           'stadsdelen': {'application': 'DGDialog'},
           'wijken': {'application': 'DGDialog'},
           'buurten': {'application': 'DGDialog'},
           'bouwblokken': {'application': 'DGDialog'},
           'ggwgebieden': {'application': 'Basisinformatie'},
           'ggpgebieden': {'application': 'Basisinformatie'}
       }
    },
    # 'bag': {
    #     'collections': {
    #         'woonplaatsen': {},
    #         'standplaatsen': {},
    #         'ligplaatsen': {},
    #         'openbareruimtes': {},
    #         'nummeraanduidingen': {},
    #         'verblijfsobjecten': {},
    #         'panden': {},
    #         'dossiers': {},
    #         'brondocumenten': {}
    #     }
    # },
    # 'bgt': {
    #     'collections': {
    #         'onderbouw': {}
    #     }
    # }
}

PIPELINES = ['import', 'relate', 'export']

DEFAULT_PIPELINE_ARGS = {
    'import': {
        'application': None
    },
    'relate': {},
    'export': {
        'destination': "Objectstore"
    }
}
