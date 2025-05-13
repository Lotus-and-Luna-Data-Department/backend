# flows/shopify_flows/returns/extract_returns.py

import logging
import json
import time
from datetime import datetime
import pytz
from typing import Tuple, List, Dict
from prefect import task

from flows.shop_flows.shop_utils import ShopifyClient, safe_get

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def _fetch_all_orders(updated_at_min: str, client: ShopifyClient) -> List[dict]:
    """
    Fetch all Shopify orders that might contain refunds,
    filtered by updated_at >= updated_at_min.
    """
    all_orders = []
    since_cursor = None
    logger.info(f"_fetch_all_orders: updated_at >= {updated_at_min}")

    # GraphQL query with sortKey updated to UPDATED_AT
    query = f"""
    query($cursor: String) {{
      orders(
        first: 100
        after: $cursor
        query: "updated_at:>={updated_at_min}"
        sortKey: UPDATED_AT
      ) {{
        edges {{
          node {{
            id
            name
            createdAt
            updatedAt
            refunds(first: 1) {{
              id
              updatedAt
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

    batch_number = 0
    max_iterations = 1000
    start_time = time.time()

    # prepare datetime threshold
    updated_at_min_dt = datetime.strptime(updated_at_min, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)

    while True:
        batch_number += 1
        logger.debug(f"_fetch_all_orders: Batch {batch_number}")
        try:
            variables = {"cursor": since_cursor}
            response = client.execute(query, variables)
            data = json.loads(response)

            if 'errors' in data:
                logger.error(f"Batch {batch_number}: GraphQL errors: {data['errors']}")
                break

            orders_data = safe_get(data, 'data', 'orders', default={})
            edges = safe_get(orders_data, 'edges', default=[])
            if not edges:
                logger.info(f"Batch {batch_number}: No edges found, ending pagination.")
                break

            # Enforce updated_at filter client-side
            filtered = []
            for edge in edges:
                node = safe_get(edge, 'node', default={})
                updated_at_val = safe_get(node, 'updatedAt', default=None)
                if not updated_at_val:
                    continue
                try:
                    node_dt = datetime.strptime(updated_at_val, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
                except Exception:
                    continue
                if node_dt < updated_at_min_dt:
                    continue
                filtered.append(node)

            all_orders.extend(filtered)
            logger.info(f"Batch {batch_number}: Fetched {len(filtered)} orders so far => total {len(all_orders)}")

            page_info = safe_get(orders_data, 'pageInfo', default={})
            if not safe_get(page_info, 'hasNextPage', default=False):
                logger.info(f"Batch {batch_number}: No more pages. Stopping.")
                break

            since_cursor = safe_get(page_info, 'endCursor')
            logger.info(f"Batch {batch_number}: Elapsed time: {time.time() - start_time:.2f}s")

            if batch_number >= max_iterations:
                logger.warning("_fetch_all_orders: Max iterations reached; stopping pagination.")
                break

        except Exception as e:
            logger.error(f"Batch {batch_number}: Error during pagination: {e}")
            time.sleep(2)
            continue

    logger.info(f"_fetch_all_orders: Completed, total fetched={len(all_orders)}")
    return all_orders

def _filter_orders_with_refunds(
    orders: List[dict], updated_at_min: str
) -> List[dict]:
    """
    Among the fetched orders, keep only those that have at least one refund
    whose updatedAt >= updated_at_min.
    """
    updated_at_min_dt = datetime.strptime(updated_at_min, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
    orders_with_refunds = []

    logger.info(f"_filter_orders_with_refunds: updated_at_min={updated_at_min}")

    for order in orders:
        refunds = safe_get(order, 'refunds', default=[])
        if refunds and len(refunds) > 0:
            # We only check the first refund node if it exists
            refund = refunds[0]
            refund_updated = safe_get(refund, 'updatedAt', default=None)
            if refund_updated:
                try:
                    processed_at_dt = datetime.strptime(refund_updated, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
                    if processed_at_dt >= updated_at_min_dt:
                        orders_with_refunds.append(order)
                        #logger.debug(f"Kept order {safe_get(order, 'id')} with refund updated at {processed_at_dt}")
                except Exception as e:
                    logger.warning(f"Error parsing refund timestamp for order {safe_get(order, 'id')}: {e}")

    logger.info(f"_filter_orders_with_refunds: returning {len(orders_with_refunds)} orders with refunds.")
    return orders_with_refunds


def _fetch_refund_details(order_ids: List[str], updated_at_min: str, client: ShopifyClient) -> Tuple[List[dict], List[dict]]:
    """
    For each order ID, fetch all refunds (and refund line items) updated after updated_at_min.
    Returns (refunds_list, refund_line_items_list).
    """
    all_refunds = []
    all_refund_line_items = []
    logger.info(f"_fetch_refund_details: updated_at_min={updated_at_min}")

    batch_size = 100
    updated_at_min_dt = datetime.strptime(updated_at_min, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)

    for i in range(0, len(order_ids), batch_size):
        batch_ids = order_ids[i:i+batch_size]
        ids_string = ','.join(f'"{oid}"' for oid in batch_ids)
        logger.debug(f"_fetch_refund_details: Batch {i // batch_size + 1}, order IDs={ids_string[:120]}")

        query = f"""
        query {{
          nodes(ids: [{ids_string}]) {{
            ... on Order {{
              id
              name
              createdAt
              updatedAt
              refunds(first: 100) {{
                id
                createdAt
                updatedAt
                totalRefundedSet {{
                  shopMoney {{
                    amount
                  }}
                }}
                refundLineItems(first: 100) {{
                  edges {{
                    node {{
                      quantity
                      lineItem {{
                        id
                        title
                        sku
                        product {{
                          id
                          productType
                        }}
                        variant {{
                          id
                          sku
                        }}
                      }}
                      subtotalSet {{
                        shopMoney {{
                          amount
                        }}
                      }}
                      totalTaxSet {{
                        shopMoney {{
                          amount
                        }}
                      }}
                    }}
                  }}
                }}
                transactions(first: 250) {{
                  edges {{
                    node {{
                      id
                      amount
                      kind
                      status
                      createdAt
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        try:
            logger.info(f"_fetch_refund_details: Executing refund details query for batch {i // batch_size + 1}")
            response = client.execute(query, {})
            data = json.loads(response)
            if 'errors' in data:
                logger.error(f"Batch {i // batch_size + 1}: GraphQL errors: {data['errors']}")
                continue

            nodes = safe_get(data, 'data', 'nodes', default=[])
            logger.debug(f"Batch {i // batch_size + 1}: Retrieved {len(nodes)} nodes")

            for node in nodes:
                if not node:
                    continue
                refunds = safe_get(node, 'refunds', default=[])
                for refund in refunds:
                    refund_id = safe_get(refund, 'id')
                    if not refund or not refund_id:
                        logger.warning(f"Refund missing ID in node {safe_get(node, 'id')}, skipping.")
                        continue

                    processed_at = safe_get(refund, 'updatedAt') or safe_get(refund, 'createdAt')
                    if not processed_at:
                        logger.warning(f"Refund {refund_id} missing timestamps, skipping.")
                        continue

                    try:
                        processed_at_dt = datetime.strptime(processed_at, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
                        if processed_at_dt < updated_at_min_dt:
                            continue
                    except Exception as e:
                        logger.warning(f"Error parsing timestamp for refund {refund_id}: {e}")
                        continue

                    # Summaries
                    total_refunded_base = float(
                        safe_get(refund, 'totalRefundedSet', 'shopMoney', 'amount', default="0") or "0"
                    )
                    refunded_subtotal = sum(
                        float(safe_get(rli_node, 'subtotalSet', 'shopMoney', 'amount', default="0") or "0")
                        for rli_edge in safe_get(refund, 'refundLineItems', 'edges', default=[])
                        for rli_node in [safe_get(rli_edge, 'node', default={})]
                    )
                    refunded_tax = sum(
                        float(safe_get(rli_node, 'totalTaxSet', 'shopMoney', 'amount', default="0") or "0")
                        for rli_edge in safe_get(refund, 'refundLineItems', 'edges', default=[])
                        for rli_node in [safe_get(rli_edge, 'node', default={})]
                    )

                    shipping_returned = 0.0
                    for tx_edge in safe_get(refund, 'transactions', 'edges', default=[]):
                        tx_node = safe_get(tx_edge, 'node', default={})
                        try:
                            tx_amount = float(safe_get(tx_node, 'amount', default="0") or "0")
                            # negative amount => shipping_returned
                            if tx_amount < 0:
                                shipping_returned += tx_amount
                        except Exception:
                            pass

                    gross_returns = refunded_subtotal
                    discounts_returned = 0.0
                    taxes_returned = refunded_tax
                    return_fees = 0.0
                    net_returns = gross_returns - discounts_returned
                    total_returns = net_returns + shipping_returned + taxes_returned

                    product_titles = ', '.join(
                        safe_get(rli_node, 'lineItem', 'title', default='Unknown')
                        for rli_edge in safe_get(refund, 'refundLineItems', 'edges', default=[])
                        for rli_node in [safe_get(rli_edge, 'node', default={})]
                        if safe_get(rli_node, 'lineItem', 'title')
                    )

                    # Create the main refund record
                    refund_record = {
                        'refund_id': refund_id,
                        'order_id': safe_get(node, 'name', default=''),
                        'created_at': safe_get(refund, 'createdAt', default=''),
                        'processed_at': processed_at,
                        'updated_at': safe_get(refund, 'updatedAt', default=''),
                        'total_refunded': total_refunded_base,
                        'refund_amount': total_refunded_base,
                        'sale_id': safe_get(node, 'name', default=''),
                        'product_title': product_titles if product_titles else 'Unknown',
                        'gross_returns': gross_returns,
                        'discounts_returned': discounts_returned,
                        'shipping_returned': shipping_returned,
                        'taxes_returned': taxes_returned,
                        'return_fees': return_fees,
                        'net_returns': net_returns,
                        'total_returns': total_returns
                    }
                    all_refunds.append(refund_record)
                    #logger.info(f"Added refund {refund_id} with total_returns={total_returns:.2f}")

                    # Now gather the line items
                    for rli_edge in safe_get(refund, 'refundLineItems', 'edges', default=[]):
                        rli_node = safe_get(rli_edge, 'node', default={})
                        if rli_node:
                            line_item_info = safe_get(rli_node, 'lineItem', default={})
                            refund_line_item_record = {
                                'refund_id': refund_id,
                                'order_id': safe_get(node, 'name', default=''),
                                'quantity': safe_get(rli_node, 'quantity', default=0),
                                'subtotal': safe_get(rli_node, 'subtotalSet', 'shopMoney', 'amount', default="0"),
                                'tax_subtotal': safe_get(rli_node, 'totalTaxSet', 'shopMoney', 'amount', default="0"),
                                'line_item_id': safe_get(line_item_info, 'id', default=''),
                                'title': safe_get(line_item_info, 'title', default=''),
                                'sku': safe_get(line_item_info, 'sku', default=''),
                                'product_id': safe_get(line_item_info, 'product', 'id', default=''),
                                'product_type': safe_get(line_item_info, 'product', 'productType', default=''),
                                'variant_id': safe_get(line_item_info, 'variant', 'id', default=''),
                                'variant_sku': safe_get(line_item_info, 'variant', 'sku', default='')
                            }
                            all_refund_line_items.append(refund_line_item_record)
                            #logger.debug(f"Added line item {safe_get(line_item_info, 'id')} for refund {refund_id}")
        except Exception as e:
            logger.error(f"Batch {i // batch_size + 1}: Error fetching refund details: {e}")
            continue

    logger.info(f"_fetch_refund_details: total_refunds={len(all_refunds)}, total_line_items={len(all_refund_line_items)}")
    return all_refunds, all_refund_line_items

@task
def extract_shopify_returns(updated_at_min: str):
    """
    Main Prefect task to extract Shopify refunds flow:
      1) fetch all relevant orders
      2) filter orders that actually have refunds
      3) fetch the refund details
    Returns: (refunds_list, refund_line_items_list)
    """
    logger.info(f"=== extract_shopify_returns: Starting extraction for updated_at>={updated_at_min} ===")
    client = ShopifyClient()

    # 1) fetch all orders
    all_orders = _fetch_all_orders(updated_at_min, client)
    # 2) filter for those that have refunds
    refund_orders = _filter_orders_with_refunds(all_orders, updated_at_min)
    if not refund_orders:
        logger.warning("No orders with refunds found after filtering.")
        return [], []

    order_ids = [safe_get(order, 'id') for order in refund_orders]
    logger.info(f"Orders with refunds: {len(order_ids)}")

    # 3) fetch refund details
    refunds_list, refund_line_items_list = _fetch_refund_details(order_ids, updated_at_min, client)
    logger.info(f"=== extract_shopify_returns complete: {len(refunds_list)} refunds, {len(refund_line_items_list)} line items ===")
    return refunds_list, refund_line_items_list
