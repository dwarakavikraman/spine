from logging.handlers import RotatingFileHandler
import frappe
import logging

from frappe.model.rename_doc import rename_doc

class DocTypeNotAvailable(Exception):
	def __init__(self, doctype, *args, **kwargs):
		self.message = "{} Doctype not available".format(doctype)
		super(Exception, self).__init__(self.message)

class UnknownEvent(Exception):
	def __init__(self, event, *args, **kwargs):
		self.message = "Cannot process unknown event {}".format(event)
		super(Exception, self).__init__(self.message)

class DocDoesNotExist(Exception):
    def __init__(self, doctype, docname, *args, **kwargs):
        self.message = "{} Document of type {} does not exist".format(docname, doctype)
        super(Exception, self).__init__(self.message)

class IncorrectData(Exception):
    pass

module_name = __name__
logger = None
LOG_FILENAME = '../logs/handler.log'

def get_module_logger():
    global logger
    if logger is not None:
        return logger

    formatter = logging.Formatter('[%(levelname)s] %(asctime)s | %(pathname)s:%(message)s\n')
    # handler = logging.StreamHandler()

    handler = RotatingFileHandler(
        LOG_FILENAME, maxBytes=100000, backupCount=20)
    handler.setFormatter(formatter)

    logger = logging.getLogger(module_name)
    logger.setLevel(frappe.log_level or logging.DEBUG)
    logger.addHandler(handler)
    logger.propagate = False

    # loggers[module] = logger
    return logger

def purge_to_file(payload):
    """
    Purging all Payload to FS
    """
    file = open(frappe.utils.get_files_path() + '/purge_msgs.{}.txt'.format(str(frappe.utils.data.get_datetime().date())), '+a') 
    file.write(str(payload) + "\n\n")
    file.close()

def handler(payload, raise_error = True):
    try:
        logger = get_module_logger()
        incoming_doctype = payload.get("Header").get("DocType")
        logger.debug(incoming_doctype)
        if not incoming_doctype or not frappe.db.exists("DocType", incoming_doctype):
            logger.debug("Doctype does not Exist ->", incoming_doctype)
            if raise_error:
                raise DocTypeNotAvailable(incoming_doctype)
            else:
                return None
        event = payload.get("Header").get("Event")
        logger.debug("Handling Event -> {}".format(event))
        if event in ["on_update", "on_update_after_submit"]:
            handle_update(payload)
        elif event in "on_submit":
            handle_submit(payload)
        elif event in "on_cancel":
            handle_cancel(payload)
        elif event in ["after_insert", "first_sync"]:
            handle_insert(payload)
        elif event == "on_trash":
            handle_remove(payload)
        elif event == "after_rename":
            handle_rename(payload)
        else:
            logger.debug("Did not find event "+event)
            if raise_error:
                raise UnknownEvent(event)
            else:
                return None
    except:
        if raise_error:
            raise

def get_local_doc(doctype, docname):
    """Get the local document if created with a different name"""
    if not doctype or not docname:
         return None
    try:
        return frappe.get_doc(doctype, docname)
    except frappe.DoesNotExistError:
        return None
    
def remove_payload_fields(payload):
    remove_fields = ["modified", "creation", "modified_by", "owner", "idx"]
    for f in remove_fields:
        if payload.get(f):
            del payload[f]
    return payload

def handle_insert_or_update(payload):
    """Sync insert or update type update"""
    logger = get_module_logger()
    doctype = payload.get("Payload").get("doctype")
    docname = payload.get("Payload").get("name")
    if not doctype or not docname:
        logger.debug("Incorrect Data passed")
        raise IncorrectData(msg="Incorrect Data passed")
    local_doc = get_local_doc(doctype, docname)
    if local_doc:
        logger.debug("Updating {}".format(docname))
        data = frappe._dict(payload.get('Payload'))
        remove_payload_fields(data)
        local_doc.update(data)
        logger.debug("Saving doc")
        local_doc.save()
        local_doc.db_update_all()
    else:
        logger.debug("Creating {}".format(docname))
        data = frappe._dict(payload.get("Payload"))
        remove_payload_fields(data)
        doc = frappe.get_doc(data)
        doc.insert(set_name=docname, set_child_names=False)

def handle_update(payload):
    """Sync update type update"""
    logger = get_module_logger()
    doctype = payload.get("Payload").get("doctype")
    docname = payload.get("Payload").get("name")
    if not doctype or not docname:
        raise IncorrectData(msg="Incorrect Data passed")
    local_doc = get_local_doc(doctype, docname)
    logger.debug("Updating {}".format(docname))
    logger.debug(local_doc)
    if local_doc:
        data = frappe._dict(payload.get('Payload'))
        remove_payload_fields(data)
        local_doc.update(data)
        logger.debug("Saving doc")
        local_doc.save()
        local_doc.db_update_all()
    else:
        raise DocDoesNotExist(doctype, docname)

def handle_insert(payload):
    doctype = payload.get("Payload").get("doctype")
    docname = payload.get("Payload").get("name")
    if not doctype or not docname:
        raise IncorrectData(msg="Incorrect Data passed")
    if frappe.db.get_value(doctype, docname):
        raise Exception("Document already exists")
    
    data = frappe._dict(payload.get("Payload"))
    remove_payload_fields(data)
    doc = frappe.get_doc(data)
    doc.insert(set_name=docname, set_child_names=False)
    return doc

def handle_submit(payload):
    doctype = payload.get("Payload").get("doctype")
    docname = payload.get("Payload").get("name")
    if not doctype or not docname:
        raise IncorrectData(msg="Incorrect Data passed")
    local_doc = get_local_doc(doctype, docname)
    if local_doc:
        local_doc.submit()

def handle_cancel(payload):
    doctype = payload.get("Payload").get("doctype")
    docname = payload.get("Payload").get("name")
    if not doctype or not docname:
        raise IncorrectData(msg="Incorrect Data passed")
    local_doc = get_local_doc(doctype, docname)
    if local_doc:
        local_doc.cancel()

def handle_remove(payload):
    doctype = payload.get("Payload").get("doctype")
    docname = payload.get("Payload").get("name")
    if not doctype or not docname:
        raise IncorrectData(msg="Incorrect Data passed")
    local_doc = get_local_doc(doctype, docname)
    if local_doc:
        local_doc.remove()

def handle_rename(payload):
    doctype = payload.get("Payload").get("doctype")
    publish_doc = payload.get("Payload")
    rename_meta = publish_doc.get("rename_meta")
    if rename_meta:
        local_doc = get_local_doc(doctype, rename_meta.get("old_name"))
        if local_doc:
            rename_doc(doctype=doctype, old=rename_meta.get("old_name"), new=rename_meta.get("new_name"), merge=rename_meta.get("merge"))
            # local_doc.rename(name=rename_meta.get("new_name"),  merge=rename_meta.get("merge"))
    else:
        raise IncorrectData("Incorrect Data Passed")

