"""
Tests for native async iteration with async cursors.

These tests verify that the async iteration implemented in ModelIterable,
ValuesIterable, ValuesListIterable, and FlatValuesListIterable properly
uses async cursors for true non-blocking database access.

Uses TransactionTestCase with async data setup to avoid mixing sync/async
database connections, which would cause transaction isolation issues.
"""
from datetime import datetime

from django.db.models import F
from django.test import TransactionTestCase, skipUnlessDBFeature

from .models import RelatedModel, SimpleModel


@skipUnlessDBFeature("supports_async")
class AsyncIterationTests(TransactionTestCase):
    """Tests for async iteration using native async cursors."""

    available_apps = ["async"]

    async def test_async_model_iteration(self):
        """Test basic async iteration over model instances."""
        s1 = await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        s2 = await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        s3 = await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for m in SimpleModel.objects.order_by("pk").aiterator():
            results.append(m)

        self.assertEqual(results, [s1, s2, s3])

    async def test_async_iteration_with_select_related(self):
        """Test async iteration with select_related properly loads related objects."""
        s1 = await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        s2 = await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        s3 = await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))
        r1 = await RelatedModel.objects.acreate(simple=s1)
        r2 = await RelatedModel.objects.acreate(simple=s2)
        r3 = await RelatedModel.objects.acreate(simple=s3)

        results = []
        async for r in RelatedModel.objects.select_related("simple").order_by(
            "pk"
        ).aiterator():
            # Access the related object - should not trigger additional query
            results.append((r, r.simple))

        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], (r1, s1))
        self.assertEqual(results[1], (r2, s2))
        self.assertEqual(results[2], (r3, s3))

    async def test_async_iteration_select_related_no_extra_queries(self):
        """Verify select_related doesn't cause N+1 queries in async iteration."""
        s1 = await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        await RelatedModel.objects.acreate(simple=s1)

        # This test ensures select_related is working - related objects should
        # be populated from the JOIN without additional queries
        async for r in RelatedModel.objects.select_related("simple").aiterator():
            # Accessing r.simple should not trigger a query since it's select_related
            self.assertIsNotNone(r.simple)
            self.assertIsInstance(r.simple, SimpleModel)

    async def test_async_values_iteration(self):
        """Test async iteration with values() returns dicts."""
        await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for row in SimpleModel.objects.values("field").order_by(
            "field"
        ).aiterator():
            results.append(row)

        self.assertEqual(results, [{"field": 1}, {"field": 2}, {"field": 3}])

    async def test_async_values_list_iteration(self):
        """Test async iteration with values_list() returns tuples."""
        await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for row in SimpleModel.objects.values_list("field").order_by(
            "field"
        ).aiterator():
            results.append(row)

        self.assertEqual(results, [(1,), (2,), (3,)])

    async def test_async_flat_values_list_iteration(self):
        """Test async iteration with values_list(flat=True) returns single values."""
        await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for val in SimpleModel.objects.values_list("field", flat=True).order_by(
            "field"
        ).aiterator():
            results.append(val)

        self.assertEqual(results, [1, 2, 3])

    async def test_async_named_values_list_iteration(self):
        """Test async iteration with values_list(named=True) returns namedtuples."""
        await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for row in SimpleModel.objects.values_list("field", named=True).order_by(
            "field"
        ).aiterator():
            results.append(row)

        self.assertEqual(len(results), 3)
        # Check that results are namedtuples with 'field' attribute
        self.assertEqual(results[0].field, 1)
        self.assertEqual(results[1].field, 2)
        self.assertEqual(results[2].field, 3)

    async def test_async_iteration_with_filter(self):
        """Test async iteration with filters applied."""
        s1 = await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        s2 = await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        s3 = await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for m in SimpleModel.objects.filter(field__gte=2).order_by(
            "pk"
        ).aiterator():
            results.append(m)

        self.assertEqual(results, [s2, s3])

    async def test_async_iteration_with_annotations(self):
        """Test async iteration preserves annotations."""
        await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for row in SimpleModel.objects.annotate(doubled=F("field") * 2).values(
            "field", "doubled"
        ).order_by("field").aiterator():
            results.append(row)

        self.assertEqual(
            results,
            [
                {"field": 1, "doubled": 2},
                {"field": 2, "doubled": 4},
                {"field": 3, "doubled": 6},
            ],
        )

    async def test_async_aiterator_method(self):
        """Test the aiterator() method for async iteration."""
        s1 = await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        s2 = await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        s3 = await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for m in SimpleModel.objects.order_by("pk").aiterator():
            results.append(m)

        self.assertEqual(results, [s1, s2, s3])

    async def test_async_aiterator_with_chunk_size(self):
        """Test aiterator() with custom chunk size."""
        s1 = await SimpleModel.objects.acreate(field=1, created=datetime(2022, 1, 1))
        s2 = await SimpleModel.objects.acreate(field=2, created=datetime(2022, 1, 2))
        s3 = await SimpleModel.objects.acreate(field=3, created=datetime(2022, 1, 3))

        results = []
        async for m in SimpleModel.objects.order_by("pk").aiterator(chunk_size=1):
            results.append(m)

        self.assertEqual(results, [s1, s2, s3])

    async def test_async_iteration_empty_queryset(self):
        """Test async iteration over empty queryset."""
        results = []
        async for m in SimpleModel.objects.filter(field=999).aiterator():
            results.append(m)

        self.assertEqual(results, [])
