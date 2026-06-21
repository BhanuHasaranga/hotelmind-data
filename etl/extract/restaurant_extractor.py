"""Extracts all restaurant-domain tables."""

from etl.extract.base_extractor import BaseExtractor


class RestaurantExtractor(BaseExtractor):
    def _tables(self) -> list[tuple[str, bool]]:
        return [
            ("food_categories",   True),
            ("menu_items",        True),
            ("restaurant_tables", True),
            ("restaurant_orders", True),
            ("order_items",       True),
        ]
