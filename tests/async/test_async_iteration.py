"""
Tests for native async iteration with async cursors.

These tests verify that the async iteration implemented in ModelIterable,
ValuesIterable, ValuesListIterable, and FlatValuesListIterable properly
uses async cursors for true non-blocking database access.
"""
from datetime import datetime

from django.test import TestCase, skipUnlessDBFeature

from .models import RelatedModel, SimpleModel


@skipUnlessDBFeature("supports_async")
class AsyncIterationTests(TestCase):
    """Tests for async iteration using native async cursors."""

    @classmethod
    def setUpTestData(cls):
        cls.s1 = SimpleModel.objects.create(
            field=1,
            created=datetime(2022, 1, 1, 0, 0, 0),
        )
        cls.s2 = SimpleModel.objects.create(
            field=2,
            created=datetime(2022, 1, 1, 0, 0, 1),
        )
        cls.s3 = SimpleModel.objects.create(
            field=3,
            created=datetime(2022, 1, 1, 0, 0, 2),
        )
        cls.r1 = RelatedModel.objects.create(simple=cls.s1)
        cls.r2 = RelatedModel.objects.create(simple=cls.s2)
        cls.r3 = RelatedModel.objects.create(simple=cls.s3)

    async def test_async_model_iteration(self):
        """Test basic async iteration over model instances."""
        results = []
        async for m in SimpleModel.objects.order_by("pk"):
            results.append(m)
        self.assertEqual(results, [self.s1, self.s2, self.s3])

    async def test_async_iteration_with_select_related(self):
        """Test async iteration with select_related properly loads related objects."""
        results = []
        async for r in RelatedModel.objects.select_related("simple").order_by("pk"):
            # Access the related object - should not trigger additional query
            results.append((r, r.simple))

        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], (self.r1, self.s1))
        self.assertEqual(results[1], (self.r2, self.s2))
        self.assertEqual(results[2], (self.r3, self.s3))

    async def test_async_iteration_select_related_no_extra_queries(self):
        """Verify select_related doesn't cause N+1 queries in async iteration."""
        # This test ensures select_related is working - related objects should
        # be populated from the JOIN without additional queries
        async for r in RelatedModel.objects.select_related("simple").order_by("pk"):
            # Accessing r.simple should not trigger a query since it's select_related
            self.assertIsNotNone(r.simple)
            self.assertIsInstance(r.simple, SimpleModel)

    async def test_async_values_iteration(self):
        """Test async iteration with values() returns dicts."""
        results = []
        async for row in SimpleModel.objects.values("field").order_by("field"):
            results.append(row)

        self.assertEqual(results, [{"field": 1}, {"field": 2}, {"field": 3}])

    async def test_async_values_list_iteration(self):
        """Test async iteration with values_list() returns tuples."""
        results = []
        async for row in SimpleModel.objects.values_list("field").order_by("field"):
            results.append(row)

        self.assertEqual(results, [(1,), (2,), (3,)])

    async def test_async_flat_values_list_iteration(self):
        """Test async iteration with values_list(flat=True) returns single values."""
        results = []
        async for val in SimpleModel.objects.values_list("field", flat=True).order_by(
            "field"
        ):
            results.append(val)

        self.assertEqual(results, [1, 2, 3])

    async def test_async_named_values_list_iteration(self):
        """Test async iteration with values_list(named=True) returns namedtuples."""
        results = []
        async for row in SimpleModel.objects.values_list("field", named=True).order_by(
            "field"
        ):
            results.append(row)

        self.assertEqual(len(results), 3)
        # Check that results are namedtuples with 'field' attribute
        self.assertEqual(results[0].field, 1)
        self.assertEqual(results[1].field, 2)
        self.assertEqual(results[2].field, 3)

    async def test_async_iteration_with_filter(self):
        """Test async iteration with filters applied."""
        results = []
        async for m in SimpleModel.objects.filter(field__gte=2).order_by("pk"):
            results.append(m)

        self.assertEqual(results, [self.s2, self.s3])

    async def test_async_iteration_with_annotations(self):
        """Test async iteration preserves annotations."""
        from django.db.models import F

        results = []
        async for row in SimpleModel.objects.annotate(
            doubled=F("field") * 2
        ).values("field", "doubled").order_by("field"):
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
        results = []
        async for m in SimpleModel.objects.order_by("pk").aiterator():
            results.append(m)

        self.assertEqual(results, [self.s1, self.s2, self.s3])

    async def test_async_aiterator_with_chunk_size(self):
        """Test aiterator() with custom chunk size."""
        results = []
        async for m in SimpleModel.objects.order_by("pk").aiterator(chunk_size=1):
            results.append(m)

        self.assertEqual(results, [self.s1, self.s2, self.s3])

    async def test_async_iteration_empty_queryset(self):
        """Test async iteration over empty queryset."""
        results = []
        async for m in SimpleModel.objects.filter(field=999):
            results.append(m)

        self.assertEqual(results, [])
