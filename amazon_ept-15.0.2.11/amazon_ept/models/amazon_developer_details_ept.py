# -*- coding: utf-8 -*-
# Part of Odoo. See LICENSE file for full copyright and licensing details.

"""
Added class and fields to store the developer details.
"""
from odoo import models, fields


class AmazonDeveloperDetailsEpt(models.Model):
    """
    Added this class to store the developer details
    """
    _name = "amazon.developer.details.ept"
    _description = 'amazon developer details ept'
    _rec_name = 'developer_id'

    developer_id = fields.Char('Developer ID')
    developer_name = fields.Char()
    developer_country_id = fields.Many2one('res.country', string='Developer Country')
