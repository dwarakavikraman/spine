# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

import frappe
from frappe.model.document import Document
from frappe.utils.background_jobs import enqueue
from spine.spine_adapter.docevents.eventhandler import handle_event_wrapped


class SpineProducerConfig(Document):
    pass

@frappe.whitelist()
def trigger_event(doctype, event, filters=None):
    doc_list = frappe.get_list(doctype, filters=filters, pluck="name")
    if not frappe.conf.developer_mode:
        enqueue(
            process_bulk_event_update,
            queue="long",
            doctype=doctype,
            docnames=doc_list,
            doc_event=event,
        )
    else:
        handle_bulk_event_update(doctype, doc_list, event)
    return doc_list

def process_bulk_event_update(doctype, docnames, doc_event):
    handle_bulk_event_update(doctype, docnames, doc_event)

def handle_bulk_event_update(doctype, docnames, event):
    for d in docnames:
        doc = frappe.get_doc(doctype, d)
        handle_event_wrapped(doc, event)