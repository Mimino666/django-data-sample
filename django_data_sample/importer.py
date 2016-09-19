from collections import defaultdict
import sys

from django.db import transaction
from django.utils import six


class Importer(object):
    '''
    Imports objects from source database to dest database.
    It respects ForeignKey and OneToOne relationships, so it
    recursively follows them and creates the related objects as well.

    It fails when there is a cyclic relationship between the objects, i.e.:
        A -> B -> C -> A
    '''

    def __init__(self, source_db, dest_db, batch_size=100, stdout=sys.stdout):
        self.source_db = source_db
        self.dest_db = dest_db
        self.batch_size = batch_size
        self.stdout = stdout

    def import_objects(self, model_2_pks):
        self.model_2_pks = defaultdict(set)
        for model, pks in six.iteritems(model_2_pks):
            self.model_2_pks[model] = set(pks)

        # topologically sort the models based on their relations
        self.topsorted_models = []
        for model in six.iterkeys(self.model_2_pks):
            self._follow_model_relations(model)

        # collect pks of related objects the import
        for model in reversed(self.topsorted_models):
            self._collect_related_pks(model)

        # clean pks of already existing objects
        for model, pks in six.iteritems(self.model_2_pks):
            existing_pks = set(model._default_manager
                .using(self.dest_db)
                .filter(pk__in=pks)
                .values_list('pk', flat=True))
            pks -= existing_pks

        # create objects
        with transaction.atomic(using=self.dest_db):
            for model in self.topsorted_models:
                self._create_objects(model)

    def _create_objects(self, model):
        pks = list(self.model_2_pks[model])
        total = len(pks)
        self.stdout.write('Importing %s new objects of %s' % (total, model._meta.label))
        for start in xrange(0, total, self.batch_size):
            end = min(total, start + self.batch_size)
            objs = model._default_manager \
                .using(self.source_db) \
                .filter(pk__in=pks[start:end])
            model._default_manager \
                .using(self.dest_db) \
                .bulk_create(objs)

    def _collect_related_pks(self, model):
        related_fields = [
            field for field in model._meta.fields
            if (field.one_to_one or field.many_to_one) and field.related_model != model]

        qs = model._default_manager \
            .using(self.source_db) \
            .filter(pk__in=self.model_2_pks[model]) \
            .values(*(field.attname for field in related_fields))

        for values in qs:
            for field in related_fields:
                related_pk = values[field.attname]
                if related_pk is not None:
                    self.model_2_pks[field.related_model].add(related_pk)

    def _follow_model_relations(self, model, pending_models=None):
        # model already processed
        if model in self.topsorted_models:
            return

        # check circular relationship
        if pending_models is None:
            pending_models = []
        elif model in pending_models:
            raise RuntimeError('Circular relationship in models detected for models: %s -> %s!' %
                (' -> '.join(pending_model._meta.label for pending_model in pending_models), model._meta.label))

        pending_models.append(model)
        for field in model._meta.fields:
            if (field.one_to_one or field.many_to_one) and field.related_model != model:
                self._follow_model_relations(field.related_model, pending_models)
        pending_models.pop()

        self.topsorted_models.append(model)
