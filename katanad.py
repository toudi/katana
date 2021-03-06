#!/bin/env python2
import argparse
from katana.service import KatanaService
import logging
from importlib import import_module

parser = argparse.ArgumentParser(description='Distributed transaction manager')
parser.add_argument('--config', default='katana.ini', help='Config file')
parser.add_argument('--verbose', action='store_true', help='Verbose mode')
commands = parser.add_subparsers(title='subcommands', description='available subcommands')
syncdb = commands.add_parser('syncdb', help='Synchronizes the database')
syncdb.set_defaults(action='syncdb')
katana = commands.add_parser('start', help='Starts katana daemon')
katana.set_defaults(action='start')
worker = commands.add_parser('worker', help='Starts the transaction worker for background processing')
worker.set_defaults(action='worker')

args = parser.parse_args()
if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

katana = KatanaService(args.__dict__)

if args.action == 'start':
    katana.run()
elif args.action == 'syncdb':
    katana.storage.sync()
elif args.action == 'worker':
    worker = import_module(katana.config.get('katana', 'worker')).Worker(
        config=katana.config,
        storage=katana.storage
    )
    worker.run()
