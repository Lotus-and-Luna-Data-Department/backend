# flows/shopify_flows/orders/extract_orders.py

import logging
import json
import time
from datetime import datetime
from typing import Tuple, List, Dict

from prefect import task

from flows.shop_flows.shop_utils import ShopifyClient, safe_get

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Ensure debug-level logs appear

def _extract_order_data(order: dict) -> dict:
    """Helper for parsing each Shopify order record."""
    try:
        gross_sales = sum(
            float(safe_get(li_node, 'originalTotalSet', 'shopMoney', 'amount', default="0"))
            for li_edge in safe_get(order, 'lineItems', 'edges', default=[])
            for li_node in [safe_get(li_edge, 'node', default={})]
        )
        shipping_line = safe_get(order, 'shippingLine', default=None)
        shipping_charges = safe_get(shipping_line, 'originalPriceSet', 'shopMoney', 'amount', default="0.00")

        shipping_charge_taxes = [
            safe_get(tl, 'price', default="0.00")
            for tl in safe_get(shipping_line, 'taxLines', default=[])
        ]

        total_tax = sum(
            float(safe_get(tl, 'price', default="0"))
            for tl in safe_get(order, 'taxLines', default=[])
            if not safe_get(tl, 'title', default='').lower().startswith('shipping')
        )

        # Extract shipping address fields
        shipping_address = safe_get(order, 'shippingAddress', default={})
        shipping_address1 = safe_get(shipping_address, 'address1', default=None)
        shipping_city = safe_get(shipping_address, 'city', default=None)
        shipping_zip = safe_get(shipping_address, 'zip', default=None)
        shipping_country_code = safe_get(shipping_address, 'countryCode', default=None)

        # Extract order-level discount applications
        discount_codes = []
        discount_total_amount = 0.0
        for da_edge in safe_get(order, 'discountApplications', 'edges', default=[]):
            da_node = safe_get(da_edge, 'node', default={})
            # Check if this is a DiscountCodeApplication to get the code
            if safe_get(da_node, '__typename', default='') == 'DiscountCodeApplication':
                code = safe_get(da_node, 'code', default=None)
                if code:
                    discount_codes.append(code)

            # Calculate total discount amount (applies to all DiscountApplication types)
            value = safe_get(da_node, 'value', default={})
            if '__typename' in value:
                if value['__typename'] == 'PricingPercentageValue':
                    percentage = safe_get(value, 'percentage', default=0.0)
                    # Approximate the discount amount using the order's total price
                    total_price = float(safe_get(order, 'currentTotalPriceSet', 'shopMoney', 'amount', default="0"))
                    discount_amount = (percentage / 100.0) * total_price
                    discount_total_amount += discount_amount
                elif value['__typename'] == 'MoneyV2':
                    amount = float(safe_get(value, 'amount', default="0.0"))
                    discount_total_amount += amount

        discount_codes_str = ",".join(discount_codes) if discount_codes else ""

        return {
            'order_id': safe_get(order, 'name', default=''),
            'created_at': safe_get(order, 'createdAt', default=''),
            'updated_at': safe_get(order, 'updatedAt', default=''),
            'display_financial_status': safe_get(order, 'displayFinancialStatus', default=''),
            'original_total_price': gross_sales,
            'current_discounts': safe_get(order, 'currentTotalDiscountsSet', 'shopMoney', 'amount', default="0"),
            'current_subtotal': safe_get(order, 'currentSubtotalPriceSet', 'shopMoney', 'amount', default="0"),
            'current_total_price': safe_get(order, 'currentTotalPriceSet', 'shopMoney', 'amount', default="0"),
            'channel_name': safe_get(order, 'channelInformation', 'channelDefinition', 'channelName', default=''),
            'sub_channel_name': safe_get(order, 'channelInformation', 'channelDefinition', 'subChannelName', default=''),
            'total_tax': str(total_tax),
            'shipping_charges': shipping_charges,
            'shipping_charge_taxes': shipping_charge_taxes,
            'carrier_identifier': safe_get(shipping_line, 'carrierIdentifier', default=None),
            # New fields
            'shipping_address1': shipping_address1,
            'shipping_city': shipping_city,
            'shipping_zip': shipping_zip,
            'shipping_country_code': shipping_country_code,
            'discount_codes': discount_codes_str,
            'discount_total_amount': discount_total_amount,
        }
    except Exception as e:
        logger.error(f"Error extracting order data: {e}")
        raise

def _extract_line_item_data(order: dict) -> List[dict]:
    """Helper for parsing line items from each order record."""
    line_items = []
    order_id = safe_get(order, 'name', default='')
    created_at = safe_get(order, 'createdAt', default='')
    updated_at = safe_get(order, 'updatedAt', default='')

    for li_edge in safe_get(order, 'lineItems', 'edges', default=[]):
        li_node = safe_get(li_edge, 'node', default={})
        try:
            # Extract discount allocations and split into codes and amounts
            discount_codes = []
            discount_amounts = []
            for da in safe_get(li_node, 'discountAllocations', default=[]):
                da_app = safe_get(da, 'discountApplication', default={})
                # Check if this is a DiscountCodeApplication to get the code
                code = None
                if safe_get(da_app, '__typename', default='') == 'DiscountCodeApplication':
                    code = safe_get(da_app, 'code', default=None)
                allocated_amount = float(safe_get(da, 'allocatedAmountSet', 'shopMoney', 'amount', default="0.0"))
                if code:  # Only include if there's a discount code
                    discount_codes.append(code)
                    discount_amounts.append(str(allocated_amount))

            # Convert lists to comma-separated strings
            discount_codes_str = ",".join(discount_codes) if discount_codes else ""
            discount_amounts_str = ",".join(discount_amounts) if discount_amounts else ""

            line_items.append({
                'order_id': order_id,
                'line_item_id': safe_get(li_node, 'id', default=''),
                'created_at': created_at,
                'updated_at': updated_at,
                'title': safe_get(li_node, 'title', default=''),
                'quantity': safe_get(li_node, 'quantity', default=0),
                'sku': safe_get(li_node, 'sku', default=''),
                'product_id': safe_get(li_node, 'product', 'id', default=''),
                'product_type': safe_get(li_node, 'product', 'productType', default=''),
                'variant_id': safe_get(li_node, 'variant', 'id', default=''),
                'variant_sku': safe_get(li_node, 'variant', 'sku', default=''),
                'variant_price': safe_get(li_node, 'variant', 'price', default="0"),
                'original_total': safe_get(li_node, 'originalTotalSet', 'shopMoney', 'amount', default="0"),
                'discounted_total': safe_get(li_node, 'discountedTotalSet', 'shopMoney', 'amount', default="0"),
                # New fields replacing discount_allocations
                'discount_codes': discount_codes_str,
                'discount_amounts': discount_amounts_str,
            })
        except Exception as e:
            logger.error(f"Error extracting line item for order {order_id}: {e}")
            continue

    return line_items

@task
def extract_shopify_orders(start_date: str) -> Tuple[List[dict], List[dict]]:
    """
    Fetch Shopify orders and line items from 'start_date' onward.
    Returns two lists: (orders_list, line_items_list).
    """
    logger.info("=== extract_shopify_orders: Starting extraction of Shopify order data ===")

    if not start_date:
        raise ValueError("'start_date' is required and cannot be empty.")

    client = ShopifyClient()
    total_fetched = 0
    batch_number = 0
    since_cursor = None

    logger.info(f"Using filter: created_at >= {start_date}")
    filter_dt = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')

    # GraphQL query with relevant filtering
    query = f"""
    query($cursor: String) {{
      orders(
        first: 100
        after: $cursor
        query: "NOT (financial_status:voided OR financial_status:pending OR financial_status:authorized) created_at:>={start_date}"
        sortKey: CREATED_AT
      ) {{
        edges {{
          node {{
            id
            name
            createdAt
            updatedAt
            displayFinancialStatus
            channelInformation {{
              channelDefinition {{
                channelName
                subChannelName
              }}
            }}
            totalTaxSet {{
              shopMoney {{
                amount
              }}
            }}
            currentSubtotalPriceSet {{
              shopMoney {{
                amount
              }}
            }}
            currentTotalDiscountsSet {{
              shopMoney {{
                amount
              }}
            }}
            currentTotalPriceSet {{
              shopMoney {{
                amount
              }}
            }}
            shippingAddress {{
              address1
              city
              zip
              countryCode
            }}
            discountApplications(first: 10) {{
              edges {{
                node {{
                  __typename
                  ... on DiscountCodeApplication {{
                    code
                  }}
                  value {{
                    ... on PricingPercentageValue {{
                      percentage
                    }}
                    ... on MoneyV2 {{
                      amount
                      currencyCode
                    }}
                  }}
                }}
              }}
            }}
            shippingLine {{
              originalPriceSet {{
                shopMoney {{
                  amount
                }}
              }}
              discountAllocations {{
                allocatedAmount {{
                  amount
                }}
              }}
              taxLines {{
                price
              }}
              carrierIdentifier
            }}
            taxLines {{
              price
              title
            }}
            lineItems(first: 250) {{
              edges {{
                node {{
                  id
                  title
                  quantity
                  sku
                  product {{
                    id
                    productType
                  }}
                  variant {{
                    id
                    sku
                    price
                  }}
                  originalTotalSet {{
                    shopMoney {{
                      amount
                    }}
                  }}
                  discountedTotalSet {{
                    shopMoney {{
                      amount
                    }}
                  }}
                  discountAllocations {{
                    allocatedAmountSet {{
                      shopMoney {{
                        amount
                      }}
                    }}
                    discountApplication {{
                      __typename
                      ... on DiscountCodeApplication {{
                        code
                      }}
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """

    orders_list: List[dict] = []
    line_items_list: List[dict] = []

    start_time = time.time()
    while True:
        batch_number += 1
        logger.debug("extract_shopify_orders: Starting batch %d", batch_number)
        try:
            variables = {"cursor": since_cursor}
            response = client.execute(query, variables)
            data = json.loads(response)

            if 'errors' in data:
                logger.error("Batch %d: GraphQL errors: %s", batch_number, data['errors'])
                break

            orders_data = safe_get(data, 'data', 'orders', default={})
            edges = safe_get(orders_data, 'edges', default=[])
            if not edges:
                logger.info("Batch %d: No edges found, ending pagination.", batch_number)
                break

            created_ats_this_batch = []
            for edge in edges:
                node = safe_get(edge, 'node', default={})
                if not node:
                    logger.warning("Batch %d: Skipping empty node.", batch_number)
                    continue

                created_at_val = safe_get(node, 'createdAt', default=None)
                if created_at_val:
                    created_at_dt = datetime.strptime(created_at_val, '%Y-%m-%dT%H:%M:%SZ')
                    # Enforce start_date filter
                    if created_at_dt < filter_dt:
                        #logger.debug("Batch %d: Skipping order older than %s", batch_number, start_date)
                        continue
                    created_ats_this_batch.append(created_at_val)

                # Extract order data + line items
                orders_list.append(_extract_order_data(node))
                line_items_list.extend(_extract_line_item_data(node))

            if created_ats_this_batch:
                min_created = min(created_ats_this_batch)
                max_created = max(created_ats_this_batch)
                logger.info("Batch %d: Orders from %s to %s", batch_number, min_created, max_created)
            else:
                logger.debug("Batch %d: No createdAt in this batch", batch_number)

            total_fetched += len(edges)
            logger.info("Batch %d: Fetched %d orders; total so far: %d", batch_number, len(edges), total_fetched)

            page_info = safe_get(orders_data, 'pageInfo', default={})
            has_next = safe_get(page_info, 'hasNextPage', default=False)
            if not has_next:
                logger.info("Batch %d: No more pages. Ending pagination.", batch_number)
                break

            since_cursor = safe_get(page_info, 'endCursor')
            logger.debug("Batch %d", batch_number)
            logger.info("Elapsed time so far: %.2f seconds", time.time() - start_time)

        except Exception as e:
            logger.error("Batch %d: Error during pagination: %s", batch_number, e)
            time.sleep(2)
            continue

    logger.info("=== extract_shopify_orders complete: total fetched = %d orders ===", total_fetched)
    logger.info("orders_list length: %d, line_items_list length: %d", len(orders_list), len(line_items_list))

    return orders_list, line_items_list