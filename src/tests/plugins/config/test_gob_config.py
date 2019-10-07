from unittest import TestCase

from plugins.config.gob_config import CATALOGUES

class TestGOBConfig(TestCase):

    def test_catalogues(self):
        self.assertIsInstance(CATALOGUES, dict)
        for catalogue_name, catalogue in CATALOGUES.items():
            self.assertIsInstance(catalogue, dict)
            for collection_name, collection in catalogue['collections'].items():
                self.assertIsInstance(collection, dict)
