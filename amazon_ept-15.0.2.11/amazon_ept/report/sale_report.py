# -*- coding: utf-8 -*-
"""
inherited sale report class and inherited method to update the query.
"""

from odoo import fields, models


class SaleReport(models.Model):
    """
    Added class to add fields to relate wth the instance, seller and selling on and
    updated query to get sale report with group by those fields.
    """
    _inherit = "sale.report"

    amz_instance_id = fields.Many2one('amazon.instance.ept', 'Marketplace', readonly=True)
    amz_seller_id = fields.Many2one('amazon.seller.ept', 'Amazon Sellers', readonly=True)
    amz_fulfillment_by = fields.Selection([('FBA', 'Fulfilled By Amazon'),
                                           ('FBM', 'Fulfilled By Merchant')],
                                          string='Fulfillment By', readonly=True)

    def _select_sale(self, field=None):
        """
        Inherited Select method to Add Amazon fields filter in Reports
        :param field:
        :return:
        """
        field['amz_instance_id'] = ", s.amz_instance_id as amz_instance_id"
        field['amz_seller_id'] = ", s.amz_seller_id as amz_seller_id"
        field['amz_fulfillment_by'] = ", s.amz_fulfillment_by as amz_fulfillment_by"
        return super(SaleReport, self)._select_sale(fields=field)


    def _group_by_sale(self, groupby=''):
        """
        Inherit group by for filter amazon data
        :param groupby:
        :return:
        """
        groupby = super(SaleReport, self)._group_by_sale(groupby=groupby)
        groupby += ", s.amz_instance_id, s.amz_seller_id, s.amz_fulfillment_by"
        return groupby
