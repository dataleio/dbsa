import os
import sys
import dbsa
import inspect
import argparse
import importlib

def main(argv=sys.argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('dialect')
    parser.add_argument('modules', nargs='+')
    parser.add_argument('--ns', action='store_true')
    args = parser.parse_args()

    dialect_module = importlib.import_module('dbsa.' + args.dialect)

    module_names, paths_to_import, moduls = [], set(), []
    for module_path in args.modules:
        pathname, filename = os.path.split(module_path)
        paths_to_import.add(os.path.abspath(pathname))
        module_name = os.path.splitext(filename)[0]
        if module_name: module_names.append(module_name)

    for pathname in paths_to_import:
        sys.path.append(pathname)

    level = 0 if args.ns is True else 1
    if level == 1:
        print('# Schema documentation')

    for module_name in module_names:
        if module_name.startswith('__'): continue
        module = importlib.import_module(module_name)
        print((level+1) * '#' + ' ' + module_name)
        print(module.__doc__ or '')
        for cls_name, cls in inspect.getmembers(module, inspect.isclass):
            if not issubclass(cls, dbsa.Table) or cls_name.startswith('__'): continue
            print(dialect_module.Table(cls(schema=module_name)).to_markdown(header='#'*(level+2)))
