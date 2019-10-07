"""
GOB Configuration

All DAGs and properties of DAGs are read from the info in this module
"""
CATALOGUES = {
    'brk': {
        'collections': {
            'kadastraleobjecten': {},
            'zakelijkerechten': {},
            'kadastralesubjecten': {},
            'tenaamstellingen': {},
            'aantekeningenrechten': {},
            'aantekeningenkadastraleobjecten': {},
            'stukdelen': {},
            'aardzakelijkerechten': {},
            'gemeentes': {},
            'meta': {}
        }
    },
    'wkpb': {
        'collections': {
            'beperkingen': {},
            'dossiers': {},
            'brondocumenten': {}
        }
    },
    'nap': {
        'collections': {
            'peilmerken': {}
        }
    },
    'meetbouten': {
        'collections': {
            'meetbouten': {},
            'metingen': {},
            'referentiepunten': {},
            'rollagen': {}
        }
    },
    'gebieden': {
        'collections': {
            'stadsdelen': {},
            'wijken': {},
            'buurten': {},
            'bouwblokken': {},
            'ggwgebieden': {},
            'ggpgebieden': {}
        }
    },
    'bag': {
        'collections': {
            'woonplaatsen': {},
            'standplaatsen': {},
            'ligplaatsen': {},
            'openbareruimtes': {},
            'nummeraanduidingen': {},
            'verblijfsobjecten': {},
            'panden': {},
            'dossiers': {},
            'brondocumenten': {}
        }
    },
    'bgt': {
        'collections': {
            'onderbouw': {}
        }
    }
}
