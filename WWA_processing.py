import os
import pandas as pd
import logging
import zipfile
from io import StringIO
import json

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.core.files.storage import FileSystemStorage
from django.core.files.uploadedfile import InMemoryUploadedFile

from file_manager.models import SampleRecord, DataAnalysisQueue, ProcessingApp


# app name, must be the same as in the database
APPNAME = "DDA, DIA and WWA (Processing)"
Process_app = ProcessingApp.objects.filter(
                    name=APPNAME).first()
three_processor = {
    "WWA_processor":{
        "name":"PD3.0 processor",
        "method_key":"WWAMethod",
        "option_key":"WWA_option",
        "file_key":"WWA_file",
        "input_file_1_ext":".pdProcessingWF",
        "input_file_2_ext":".pdConsensusWF",
        "input_file_3_ext":".method",
        "record_list":"group1_record",
        "libary_list":"group4_record",
        "anaylsis_queue": None,
        "system_default":"preset_1"
            
            
            },
    "DDA_processor":{
        "name":"PD3.0 processor",
        "method_key":"DDAMethod",
        "option_key":"DDA_option",
        "file_key":"DDA_file",
        "input_file_1_ext":".pdProcessingWF",
        "input_file_2_ext":".pdConsensusWF",
        "input_file_3_ext":".method",
        "record_list":"group2_record",
        "libary_list":"group4_record",
        "anaylsis_queue": None,
        "system_default":"preset_2"

    },   
    
    "DIA_processor":{
        "name":"FragPipe processor",
        "method_key":"DIAMethod",
        "option_key":"DIA_option",
        "file_key":"DIA_file",
        "input_file_1_ext":".workflow",
        "input_file_2_ext":".json",
        "input_file_3_ext":".none",
        "record_list":"group3_record",
        "libary_list":"group4_record",
        "anaylsis_queue": None,
        "system_default":"preset_3"


    }
    }


# folder to store the methods
APPFOLDER = "media/primary_storage/systemfiles/DDA_DIA_and_WWA/"

# create app folder if not exist
for item in [APPFOLDER, APPFOLDER + three_processor["WWA_processor"]["method_key"], APPFOLDER + three_processor["DDA_processor"]["method_key"], APPFOLDER + three_processor["DIA_processor"]["method_key"]]:
    if not os.path.exists(item):
        os.makedirs(item)
logger = logging.getLogger(__name__)

# app view

@ login_required
def view(request):
    """_this function renders the page for the app_
    Tasks:
        1. create analysis queue for each processor, WWA, DDA, and DIA
        2. create a analysis queue based on this app
        3. link this analysis queue to the other three analysis queues
    """
    
    args = {
        'SampleRecord':
        SampleRecord.objects.order_by('-pk'),
        three_processor["WWA_processor"]["method_key"]: [f for f in os.listdir(
             APPFOLDER + three_processor["WWA_processor"]["method_key"]) if f.endswith('.zip')],
        three_processor["DDA_processor"]["method_key"]: [f for f in os.listdir(
            APPFOLDER + three_processor["DDA_processor"]["method_key"]) if f.endswith('.zip')],
        three_processor["DIA_processor"]["method_key"]: [f for f in os.listdir(
            APPFOLDER + three_processor["DIA_processor"]["method_key"]) if f.endswith('.zip')],
    }


# selection of the process/consensus/quantify configurations from uploaded or
# existing files. save the uploaded files (process/consensus/quantify) to
# APPFOLDER for future use if save is checked and new method file is uploaded


# submit by button with name "submit_run", create three processing queues for WWA, DDA, and DIA
    if request.method == 'POST' and 'submit_run' in request.POST:
        
    # creat analysis queue for each processor, WWA, DDA, DIA
        for item in ["WWA_processor", "DDA_processor","DIA_processor"]:
            processor_name = ProcessingApp.objects.filter(
                name=three_processor[item]["name"]).first()
            # file is newly uploaded
            if (len(request.FILES) != 0 and 
                    request.POST.get(three_processor[item]["option_key"]) == "custom"):
                method_zip= request.FILES[three_processor[item]["file_key"]]
                if request.POST.get('keep_method') == "True":
                    fs = FileSystemStorage(location=APPFOLDER + three_processor[item]["method_key"])
                    fs.save(method_zip.name, method_zip)
            #if none is selected
            elif request.POST.get(three_processor[item]["option_key"]) == "None":
                method_zip = None
            #if system default is selected
            elif request.POST.get(three_processor[item]["option_key"]) == "system_default":
                method_zip = getattr(Process_app, three_processor[item]["system_default"])
            #if a file is selected
            else:
                method_name = request.POST.get(three_processor[item]["option_key"])
                method_zip = APPFOLDER + three_processor[item]["method_key"] + "/" + method_name

        # read file from the zip file
            if method_zip:
                method_file_1 = None
                method_file_2 = None
                method_file_3 = None
                with zipfile.ZipFile(method_zip, 'r') as z:
                    for target_file in z.namelist():
                        if target_file.endswith(three_processor[item]["input_file_1_ext"]): # input file 1
                            path = z.extract(
                                target_file,
                                os.path.join(
                                    settings.MEDIA_ROOT,
                                    "primary_storage/"
                                    "dataqueue/uniquetempfolder"))
                            method_file_1 = InMemoryUploadedFile(open(
                            path, 'r'), None, target_file, None, None, None)
                        if target_file.endswith(three_processor[item]["input_file_2_ext"]): # consensor method
                            path = z.extract(
                                target_file,
                                os.path.join(
                                    settings.MEDIA_ROOT,
                                    "primary_storage/"
                                    "dataqueue/uniquetempfolder"))
                            method_file_2 = InMemoryUploadedFile(open(
                            path, 'r'), None, target_file, None, None, None)
                        if target_file.endswith(three_processor[item]["input_file_3_ext"]):         # quantify method
                            path = z.extract(
                                target_file,
                                os.path.join(
                                    settings.MEDIA_ROOT,
                                    "primary_storage/"
                                    "dataqueue/uniquetempfolder"))
                            method_file_3 = InMemoryUploadedFile(open(
                            path, 'r'), None, target_file, None, None, None)

            
                # all the input files
                input_files = {
                    "input_file_1": method_file_1,
                    "input_file_2": method_file_2,
                    "input_file_3": method_file_3,
                }

                newqueue = {
                    "processing_name": request.POST.get('new_analysis_name') + "_sub_"+item,
                    'processing_app': processor_name,
                    'process_creator': request.user,
                }
                if (item == "DIA_processor"): 
                    # FOR DIA processor using fragpipe, use method_file_2 to create manifest file
                    parameters = handle_uploaded_file(method_file_2)
                    result = []
                    # for the record runs and library runs
                    for record in ["record_list", "libary_list"]:
                        for i in range(len(request.POST.getlist(three_processor[item][record]))):
                            file_extension = os.path.splitext(
                                SampleRecord.objects.filter(
                                    pk=request.POST.getlist(three_processor[item][record])[i]).first(
                                ).newest_raw.file_location.name)[1]
                            inner_list = ["ThisistempfoldeR/" +
                                            request.POST.getlist(three_processor[item][record])[i] +
                                            file_extension,
                                            parameters["experiment"],
                                        parameters["bioreplicate"],
                                            parameters["data_type"]]
                            result.append(inner_list)
                    
                    # Create a StringIO object to write to
                    file = StringIO()

                    # Write the data to the StringIO object
                    for row in result:
                        file.write("\t".join([str(x) for x in row]))
                        file.write("\n")

                    # Reset the position of the StringIO object to the beginning
                    file.seek(0)

                    # Create an InMemoryUploadedFile from the StringIO object
                    manifest_memory_file = InMemoryUploadedFile(file, None,
                                                                "input2.fp-manifest",
                                                                "text/csv",
                                                                file.tell(), None)
                    newqueue["input_file_1"] =method_file_1
                    newqueue["input_file_2"] = manifest_memory_file
                else:  # FOR WWA, DDA, both using PD 3.0 processor attach the valid input files to the queue
                    for key, value in input_files.items():
                        if value:
                            newqueue[key] = value
        # crate a data analysis queue, attach the sample records to the queue,
        # and update the quanlity check.
            newtask = DataAnalysisQueue.objects.create(**newqueue, )
            three_processor[item]['analysis_queue'] = newtask.pk
            for n in request.POST.getlist(three_processor[item]['record_list']):
                newtask.sample_records.add(
                    SampleRecord.objects.filter(pk=n).first())
            for n in request.POST.getlist(three_processor[item]['libary_list']): # add teh library
                newtask.sample_records.add(
                    SampleRecord.objects.filter(pk=n).first())
        newqueue = {
                "processing_name": request.POST.get('new_analysis_name'),
                'processing_app': Process_app,
                'process_creator': request.user,
                'output_QC_number_1': three_processor["WWA_processor"]["analysis_queue"],
                'output_QC_number_2': three_processor["DDA_processor"]["analysis_queue"],
                'output_QC_number_3': three_processor["DIA_processor"]["analysis_queue"],
                
            }
        newtask = DataAnalysisQueue.objects.create(**newqueue, )


# render the page
    return render(request,
                  'filemanager/WWA_DDA_DIA_Comparison.html', args)


def post_processing(queue_id):
    """_NOT USED, this function starts once 3rd party app finished, can be used to
    extract information for the QC numbers, etc._
    """
    pass


def handle_uploaded_file(file: InMemoryUploadedFile):
    # Read the file content
    file_content = file.read()
    
    # Check if the content is bytes and decode if necessary
    if isinstance(file_content, bytes):
        file_content_str = file_content.decode('utf-8')
    else:
        file_content_str = file_content
    
    # Parse the JSON content
    json_data = json.loads(file_content_str)
    
    return json_data