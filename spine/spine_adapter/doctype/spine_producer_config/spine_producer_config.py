# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

import frappe
from frappe.model.document import Document
from frappe.utils.background_jobs import enqueue
from spine.spine_adapter.docevents.eventhandler import handle_event_wrapped
from spine.utils import get_kafka_conf


class SpineProducerConfig(Document):
    pass


@frappe.whitelist()
def trigger_event(doctype, event, filters=None, enqueue_after_commit=False):
    doc_list = frappe.get_list(doctype, filters=filters, pluck="name")
    if not frappe.conf.developer_mode:
        enqueue(
            process_bulk_event_update,
            queue="long",
            doctype=doctype,
            docnames=doc_list,
            doc_event=event,
            enqueue_after_commit=enqueue_after_commit
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

@frappe.whitelist()
def clear_message_log(filters=None):
    if not filters: frappe.throw("Please Set some filters")
    enqueue(
        _clear_message_log,
        queue="long",
        filters=filters,
    )

def _clear_message_log(filters):
    doc_list = frappe.get_list("Message Log", filters=filters,fields=["name", "last_error"])
    for d in doc_list:
        try:
            frappe.delete_doc(
                doctype="Message Log",
                name=d.name,
                ignore_on_trash=True,
                delete_permanently=True,
                ignore_missing=True,
            )
            if d.last_error:
                frappe.delete_doc(
                    doctype="Error Log",
                    name=d.last_error,
                    ignore_on_trash=True,
                    delete_permanently=True,
                    ignore_missing=True,
                )
            frappe.db.commit()
        except Exception:
            frappe.db.rollback()
        
