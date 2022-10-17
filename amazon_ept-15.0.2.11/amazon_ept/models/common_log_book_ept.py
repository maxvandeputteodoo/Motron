# -*- coding: utf-8 -*-
# See LICENSE file for full copyright and licensing details.

"""
Inherited class to add common method to create amazon transaction logs
"""
import time
import os
import base64
from io import BytesIO
from odoo import models, fields
from odoo.tools.misc import xlsxwriter


class CommonLogBookEpt(models.Model):
    """
    Inherited class to store define the common method to create log
    """
    _inherit = 'common.log.book.ept'

    active_product_attachment_id = fields.Many2one('ir.attachment', string="Attachment Id",
                                                   help="Id of attachment record", readonly=True)
    is_active_product_list = fields.Boolean(string="Active Product List", readonly=True,
                                            help="Boolean set if Active Product List Model",
                                            compute="_compute_is_active_product_list", default=False)

    def unlink(self):
        """
        Delete ir.attachment record when user delete active
        product log record by using active_product_attachment_id field

        @author : Kishan Sorani
        :return:
        """
        if self.active_product_attachment_id:
            self.active_product_attachment_id.unlink()
        return super(CommonLogBookEpt, self).unlink()

    def amazon_create_transaction_log(self, log_type, model_id, res_id):
        """
        will create an amazon log rec
        """
        log_vals = {
            'active': True,
            'model_id': model_id,
            'type': log_type,
            'res_id': res_id,
            'module': 'amazon_ept',
        }
        log_rec = self.create(log_vals)
        return log_rec

    def amazon_search_or_create_transaction_log(self, log_type, model_id, res_id):
        """
        Define method for search or create Amazon transaction logs.
        :param : type : transaction type
        :param : model_id : model related to log
        :param : res_id : here we can set record id
        :return : common.log.book.ept() object
        @author : kishan sorani on date 28-Sep-2021
        """
        log_rec = self.search([('model_id', '=', model_id), ('res_id', '=', res_id),
                               ('module', '=', 'amazon_ept')], limit=1)
        if not log_rec:
            log_rec = self.amazon_create_transaction_log(log_type, model_id, res_id)
        return log_rec

    def _compute_is_active_product_list(self):
        """
        This method set is_active_prodcut_list boolean field
        1) if active record model is active.product.listing.report.ept
           and created log lines records with product title and seller sku
           set true

        2) else set false
        :return:
        @author : Kishan Sorani
        """

        if self.model_id.model == 'active.product.listing.report.ept':
            if self.log_lines.filtered(lambda line: line.default_code and line.product_title):
                self.is_active_product_list = True
            else:
                self.is_active_product_list = False
        else:
            self.is_active_product_list = False

    def get_mismatch_report(self):
        """
        Will download excel report of mismatch details.
        @updtae by : Kishan Sorani
        :return: An action containing URL of excel attachment or bool.
        """
        self.ensure_one()
        # Get filestore path of an attachment, model_id and log.
        filestore = self.env["ir.attachment"]._filestore()
        model_id = self.env['ir.model']._get('active.product.listing.report.ept').id
        log = self.env["common.log.book.ept"].search([('res_id', '=', self.res_id),
                                                      ('model_id', '=', model_id)])
        active_product_record = self.env["active.product.listing.report.ept"].search([('id', '=', self.res_id)])

        # Create an excel file at filestore location of mismatched records.
        _file = filestore + "/Mismatch_Details_{0}.xlsx".format(time.strftime("%d_%m_%Y|%H_%M_%S"))
        workbook = xlsxwriter.Workbook(_file)
        header_style = workbook.add_format({'bold': True})
        header_fields = ["Title", "Internal Reference",
                         "Seller SKU",
                         "Marketplace", "Fulfillment"]

        # Write data to that excel file.
        worksheet = workbook.add_worksheet()
        worksheet.set_column(0, 0, 60)
        worksheet.set_column(1, 4, 30)
        for column_number, cell_value in enumerate(header_fields):
            worksheet.write(0, column_number, cell_value, header_style)
        for row, log_line in enumerate(log.log_lines, start=1):
            if log_line.product_title and log_line.default_code:
                log_line = [log_line.product_title,
                            ' ',
                            log_line.default_code,
                            active_product_record.instance_id.marketplace_id.name,
                            log_line.fulfillment_by]

                for column, cell_value in enumerate(log_line):
                    worksheet.write(row, column, cell_value)
        workbook.close()
        # Open that excel file for reading purpose and create
        # File pointer for same.
        excel_file = open(_file, "rb")
        file_pointer = BytesIO(excel_file.read())
        file_pointer.seek(0)
        if not self.active_product_attachment_id:
            # Bind that created file to attachment and then close
            # File and file pointers and then delete file from filestore.
            new_attachment = self.env["ir.attachment"].create({
                "name": "Mismatch_Details_{0}.xlsx".format(time.strftime("%d_%m_%Y|%H_%M_%S")),
                "datas": base64.b64encode(file_pointer.read()),
                "type": "binary"
            })
            file_pointer.close()
            excel_file.close()
            os.remove(_file)
            self.write({'active_product_attachment_id': new_attachment.id})
        return {
            'type': 'ir.actions.act_url',
            'url': '/web/content/%s?download=true' % self.active_product_attachment_id.id,
            'target': 'self'
        }
