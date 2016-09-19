from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from django.utils.six.moves import input

from ...importer import Importer


class Command(BaseCommand):
    help = 'Import a sample of DB objects from source DB to dest DB. Follow foreign key relations.'

    def add_arguments(self, parser):
        parser.add_argument('source_db', type=str,
            help='Name of the source database, from which to fetch the objects.')
        parser.add_argument('dest_db', type=str,
            help='Name of the destination database, to which to import the objects.')
        parser.add_argument('args', metavar='app_label[.ModelName]', nargs='*',
            help='Restricts data to the specified app_label or app_label.ModelName.')

        parser.add_argument('-e', '--exclude', dest='exclude', action='append', default=[],
            help='An app_label or app_label.ModelName to exclude '
                 '(use multiple --exclude to exclude multiple apps/models).')
        parser.add_argument('--limit', default=1000, dest='limit', type=int,
            help='Number of objects to import from each model. If 0, then import ALL objects.')
        parser.add_argument('--batch-size', default=100, dest='batch_size', type=int,
            help='Number of objects to create at once.')
        parser.add_argument('--random', action='store_true', default=False,
            help='Select random objects to fetch from DB.')
        parser.add_argument('--noinput', '--no-input',
            action='store_false', dest='interactive', default=True,
            help='Tells Django to NOT prompt the user for input of any kind.')

    def handle(self, *app_labels, **options):
        source_db = options.get('source_db')
        dest_db = options.get('dest_db')
        excludes = options.get('exclude')
        limit = options.get('limit') or None
        batch_size = options.get('batch_size')
        is_random = options.get('random')
        interactive = options.get('interactive')

        if source_db not in connections:
            raise CommandError('Unknown source DB: %s' % source_db)
        if dest_db not in connections:
            raise CommandError('Unknown dest DB: %s' % dest_db)
        if source_db == dest_db:
            raise CommandError('Source DB and dest DB must be different.')

        model_list = self._collect_models(app_labels, excludes)

        if interactive:
            confirm = input('''You have requested to import data

    from "%s" (%s)
    to "%s" (%s)

Data will be imported for the following models:
%s

Do you want to continue?

    Type 'yes' to continue, or 'no' to cancel: ''' % (
                source_db, self._format_connection(source_db),
                dest_db, self._format_connection(dest_db),
                '\n'.join('\t %s' % model._meta.label for model in model_list)))
        else:
            confirm = 'yes'

        if confirm == 'yes':
            importer = Importer(source_db, dest_db, batch_size, self.stdout)
            model_2_pks = self._collect_pks(source_db, model_list, limit, is_random)
            importer.import_objects(model_2_pks)
        else:
            self.stdout.write('Data import cancelled.')

    def _format_connection(self, db_name):
        return '%(NAME)s - %(USER)s@%(HOST)s:%(PORT)s' % connections[db_name].settings_dict

    def _collect_models(self, app_labels, excludes):
        excluded_apps = set()
        excluded_models = set()
        for exclude in excludes:
            if '.' in exclude:
                try:
                    model = apps.get_model(exclude)
                except LookupError:
                    raise CommandError('Unknown model in excludes: %s' % exclude)
                excluded_models.add(model)
            else:
                try:
                    app_config = apps.get_app_config(exclude)
                except LookupError as e:
                    raise CommandError(str(e))
                excluded_apps.add(app_config)

        # collect model list to import
        model_list = []
        if len(app_labels) == 0:
            for app_config in apps.get_app_configs():
                if app_config.models_module is not None and app_config not in excluded_apps:
                    for model in app_config.get_models():
                        if model not in excluded_models:
                            model_list.append(model)
        else:
            for label in app_labels:
                try:
                    app_label, model_label = label.split('.')
                    try:
                        app_config = apps.get_app_config(app_label)
                    except LookupError as e:
                        raise CommandError(str(e))
                    if app_config.models_module is None or app_config in excluded_apps:
                        continue
                    try:
                        model = app_config.get_model(model_label)
                    except LookupError:
                        raise CommandError('Unknown model: %s.%s' % (app_label, model_label))
                    if model not in excluded_models:
                        model_list.append(model)

                except ValueError:
                    # This is just an app - no model qualifier
                    app_label = label
                    try:
                        app_config = apps.get_app_config(app_label)
                    except LookupError as e:
                        raise CommandError(str(e))
                    if app_config.models_module is None or app_config in excluded_apps:
                        continue
                    for model in app_config.get_models():
                        if model not in excluded_models:
                            model_list.append(model)

        return model_list

    def _collect_pks(self, db, model_list, limit, is_random):
        model_2_pks = {}
        for model in model_list:
            qs = model._default_manager \
                .using(db) \
                .values_list('pk', flat=True)
            if is_random:
                qs = qs.order_by('?')
            model_2_pks[model] = qs[:limit]
        return model_2_pks
