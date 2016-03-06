import types
import unittest

from aorta.backends import load
from aorta.backends import BaseMessagingBackend
from aorta.backends.mock import MockMessagingBackend


class BackendLoadingTestCase(unittest.TestCase):

    def test_invalid_type_for_module_attribute(self):
        module = types.ModuleType('mock')
        module.MessagingBackend = object
        self.assertRaises(TypeError, load, module)

    def test_load_by_module(self):
        backend = load('aorta.backends.mock')
        self.assertTrue(isinstance(backend, MockMessagingBackend))

    def test_load_by_class_name(self):
        backend = load('aorta.backends.mock.MockMessagingBackend')
        self.assertTrue(isinstance(backend, MockMessagingBackend))

    def test_load_as_class(self):
        backend = load(MockMessagingBackend)
        self.assertTrue(isinstance(backend, MockMessagingBackend))

    def test_load_as_instance(self):
        backend = load(MockMessagingBackend())
        self.assertTrue(isinstance(backend, MockMessagingBackend))

    def test_load_missing_module_attribute(self):
        self.assertRaises(ImportError, load, 'aorta')

    def test_load_missing_class(self):
        self.assertRaises(ImportError, load, 'aorta.MessagingBackend')

    def test_load_missing_module_and_class(self):
        self.assertRaises(ImportError, load, 'foo.bar')

    def test_load_raises_on_invalid_class_provided(self):
        self.assertRaises(TypeError, load, int)

    def test_load_raises_on_invalid_instance_provided(self):
        self.assertRaises(TypeError, load, 1)

    def test_load_raises_on_existing_attribute_but_invalid_type(self):
        self.assertRaises(TypeError, load, 'aorta.backends.ISender')


if __name__ == '__main__':
    unittest.main()
