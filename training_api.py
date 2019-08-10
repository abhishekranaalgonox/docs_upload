import argparse
import json
import math
import re
import requests

from dateutil.parser import parse
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from nltk import edit_distance
from pprint import pprint


try:
    from app.db_utils import DB
    from app.producer import produce
    from app.extracto_utils import *
    from app.testing_extract import *
except:
    from db_utils import DB
    from producer import produce
    from extracto_utils import *
    from testing_extract import *

from app import app

def merge_highlights(box_list,page_number=0):
    '''
    Merge 2 or more words and get combined coordinates
    '''
    max_height = -1
    min_left = 100000
    max_right = 0
    total_width = 0
    word = ''
    top = -1


    if box_list and type(box_list[0]) is dict:
        for box in box_list:
            try:
                max_height = max(box['height'], max_height)
                min_left = min(box['left'], min_left)
                max_right = max(box['right'], max_right)
                total_width += box['width']
                word += ' ' + box['word']
                top = box['top']
            except:
                continue

        return {'height': max_height, 'width': total_width, 'y': top, 'x': min_left, 'right':max_right, 'word': word.strip(), 'page':page_number}
    else:
        return {}


def get_highlights(value, ocr_data,scope,page_no):
    try:
        value = [val.lower() for val in value.split()]
    except:
        value = ''
    ocr_data_box = ocrDataLocal(int(scope['y']),int(scope['x']),int(scope['x']+scope['width']),int(scope['y']+scope['height']),ocr_data[page_no])
    value_ocr = []
    for word in ocr_data_box:
        if word['word'].lower() in value:
            value_ocr.append(word)
    return merge_highlights(value_ocr,page_no)

def get_area_intersection(box, word, area_of_word):
    box_l,box_r,box_b,box_t = box
    word_l,word_r,word_b,word_t = word

    mid_x = box_l+(box_r - box_l)/2
    mid_y = box_t+(box_b - box_t)/2

    width = box_r - box_l
    height = box_b - box_t

    margin_wid = (width*5)/100
    margin_hig = (height*5)/100

    #this means that word is can be too big for the box
    if (word_l >= box_l and word_l <= mid_x + margin_wid):
        dx = word_r - word_l
    else:
        dx = min(word_r, box_r) - max(word_l, box_l)

    if(word_t >= box_t and word_t <= mid_y + margin_hig):
        dy = word_b - word_t
    else:
        dy = min(word_b, box_b) - max(word_t, box_t)

    if (dx>=0) and (dy>=0):
        return dx*dy

    return 0

def percentage_inside(box, word):
    '''
    Get how much part of the word is inside the box
    '''
    box_l,box_r,box_b,box_t = box
    word_l,word_r,word_b,word_t = word
    area_of_word = (word_r - word_l) * (word_b - word_t)
    area_of_intersection = get_area_intersection(box, word, area_of_word)
    try:
        return area_of_intersection/area_of_word
    except:
        return 0

def standardize_date(all_data, input_format=[r'%d-%m-%Y', r'%d.%m.%Y', r'%d/%m/%Y'], standard_format=r'%Y-%m-%d'):
    # Find date related fields and change the format of the value to a standard one
    print(f'Changing date formats in extracted fields...')
    #date_formats = [r'%d-%m-%Y', r'%d.%m.%Y', r'%d/%m/%Y']

    standard_format = r'%Y-%m-%d'
    for field_name, field_value in all_data.items():
        if 'date' in field_name.lower().split():
            if field_value is not None or field_value:
                new_field_value = field_value
                raw_field_value = field_value.replace('suspicious', '')
                try:
                    parsed_date = parse(raw_field_value, fuzzy=True, dayfirst=True)
                except ValueError:
                    print(f'Error occured while parsing date field `{field_name}`:`{field_value}`.')
                    parsed_date = None

                if parsed_date is not None:
                    if 'suspicious' in field_value:
                        new_field_value = 'suspicious' + parsed_date.strftime(standard_format)
                    else:
                        new_field_value = parsed_date.strftime(standard_format)
                all_data[field_name] = new_field_value
        if "invoice number" in field_name.lower():
            if field_value is not None or field_value:
                try:
                    all_data[field_name] = field_value.replace(' ','')
                except:
                    all_data[field_name] = field_value + 'suspicious'        
        if "gstin" in field_name.lower():
            if field_value is not None or field_value:
                pattern = r"\d{2}[a-zA-Z]{5}\d{4}[a-zA-Z]{1}\d{1}[a-zA-Z]{1}\w"
                try:
                    valid_gstin = re.findall(pattern,field_value.replace('suspicious',''))[-1]
                    all_data[field_name] = valid_gstin
                except:
                    all_data[field_name] = field_value + 'suspicious'
        if "po number" in field_name.lower():
            if field_value is not None or field_value:
                try:
                    all_data[field_name] = field_value.replace('.','').replace(':','')[:10]
                except:
                    all_data[field_name] = field_value + 'suspicious'
        if field_name.lower() in ['invoice base amount', 'invoice total']:
            try:
                all_data[field_name] = float(''.join(re.findall(r'[0-9\.]', field_value.replace('suspicious',''))))
            except:
                all_data[field_name] = field_value + 'suspicious'
    return all_data

def correct_keyword(ocr_data, keyword_sentence, scope,value):
    # Correct the last word of the keyword sentence
    # If ocr has "Invoice No:.", and keyword was trained as "Invoice No", this will make sure ocr word is saved as keyword

    junk=''
    val_to_check=''
    if value:
        val_to_check=value.split()[0]

    kwList=keyword_sentence.split()
    box_t = scope['y']
    box_r = scope['x'] + scope['width']
    box_b = scope['y'] + scope['height']
    box_l = scope['x']

    if len(kwList)>1:
        for val in ocr_data:
            if val['top']>=box_t and val['bottom']<=box_b and val['right']<=box_r and val['left']>=box_l:
                if kwList[-1] in val['word'] and val['word'] not in kwList:
                    if edit_distance(val['word'],kwList[-1]) <=3:
                        kwList[-1]=val['word']
                    elif val_to_check:
                        if val_to_check in val['word']:
                            junk=kwList[-1]
                            kwList.pop(-1)
                    else:
                        kwList.pop(-1)

    return ' '.join(kwList),junk

def keyword_extract(ocr_data, keyword, scope):
    '''
    Get closest keyword to the trained keyword.
    '''
    regex = re.compile(r'[@_!#$%^&*()<>?/\|}{~:;]')
    keyList=keyword.split()
    keyLength=len(keyList)
    keyCords=[]
    counter=0

    print(keyList)
    if keyList:
        # Search OCR for the key pattern
        for i, data in enumerate(ocr_data):
            ocr_length=len(ocr_data)
            check=False
            data['word'] = data['word'].strip()
            if(data['word']==keyList[0] or (regex.search(data['word'])!=None and keyList[0] in data['word'] )):
                if(keyLength>1):
                    for x in range(0,keyLength):
                        if (i+x) >= ocr_length:
                            check=False
                            break
                        else:
                            if(ocr_data[i+x]['word']==keyList[x] or (regex.search(ocr_data[i+x]['word'])!=None and  keyList[x] in ocr_data[i+x]['word'])):
                                check=True
                            else:
                                check=False
                                break
                else:
                    check=True

            tempCords=[{}]*1
            if(check):
                counter=counter+1
                top=10000
                bottom=0
                # Left is of the first word
                if(data['word']==keyList[0] or (regex.search(data['word'])!=None and keyList[0] in  data['word'] )):
                    tempCords[0]['left']=data['left']
                    for x in range(0,keyLength):
                        # Right is of the last word
                        if(x==(keyLength-1)):
                            tempCords[0]['right']=ocr_data[i+x]['right']

                        # If multi word key
                        if(keyLength>1):
                            if(ocr_data[i+x]['word']==keyList[x]):
                                if(ocr_data[i+x]['top']<top):
                                    top=ocr_data[i+x]['top']
                                if(ocr_data[i+x]['bottom']>bottom):
                                    bottom=ocr_data[i+x]['bottom']
                        else:
                            top=data['top']
                            bottom=data['bottom']

                    tempCords[0]['top']=top
                    tempCords[0]['bottom']=bottom
                    keyCords.append(tempCords[0])

    if(counter>0):
        keysDict=keyCords
        proceed=True
        #First try to find keyword inside the trained box
        pi=[]
        for i,values in enumerate(keysDict):
            trained_box=[scope['x'],scope['x']+scope['width'],scope['y']+scope['height'],scope['y']]
            keysDict_box=[keysDict[i]['left'],keysDict[i]['right'],keysDict[i]['bottom'],keysDict[i]['top']]
            pi.append(percentage_inside(trained_box,keysDict_box))
        maxpi=max(pi)
        if maxpi > 0.9:
            minIndex=pi.index(maxpi)
            proceed=False

        if proceed:
            print("Finding nearest to trained..")
            #Find keyword nearest to trained box
            inpX=(scope['y']+scope['y']+scope['height'])/2
            inpY=(scope['x']+scope['x']+scope['width'])/2
            DistList=[]
            pi=[]
            for i,values in enumerate(keysDict):
                    # Store all keywords,distances in a Dict
                    # Get midpoint of the input
                    midheight=((keysDict[i]['top']+keysDict[i]['bottom'])/2)
                    midwidth=((keysDict[i]['left']+keysDict[i]['right'])/2)
                    x=abs(midheight-inpX)
                    y=abs(midwidth-inpY)
                    dist=math.sqrt((x*x)+(y*y))
                    DistList.append(round(dist, 2))
            closestKey=min(DistList)
            minIndex=DistList.index(closestKey)

        key_top=keyCords[minIndex]['top']
        key_bottom=keyCords[minIndex]['bottom']
        key_left=keyCords[minIndex]['left']
        key_right=keyCords[minIndex]['right']

        return  {'height': key_bottom-key_top, 'width': key_right-key_left, 'y': key_top, 'x': key_left }

    else:
        print('keyword not found in OCR')
        return {}

def get_cell_data(scope_,multi_way_field_info,resize_factor,ocr_data):
    scope = scope_.copy()
    cell_data = {}
    for each_additional_key in multi_way_field_info['coordinates']:
        value_box = {}

        '''Resizing keywords coordinates'''
        each_additional_key = resize_coordinates(each_additional_key,resize_factor)

        each_additional_key['top'] = each_additional_key['y']
        each_additional_key['left'] = each_additional_key['x']
        each_additional_key['right'] = each_additional_key['x'] + each_additional_key['width']
        each_additional_key['bottom'] = each_additional_key['y'] + each_additional_key['height']
        value_box['top'] = scope['y']
        value_box['left'] = scope['x']
        value_box['bottom'] = scope['y']+scope['height']
        value_box['right'] = scope['x']+scope['width']

        context_ocr_data = ocrDataLocal(each_additional_key['y'],each_additional_key['x'],each_additional_key['x']+each_additional_key['width'],each_additional_key['y']+each_additional_key['height'],ocr_data)
        context_text = ' '.join([word['word'] for word in context_ocr_data])
        each_additional_key['keyword'] = context_text
        direction = get_rel_info(each_additional_key,value_box,'direction')
        try:
            cell_data[direction] = each_additional_key
        except Exception as e:
            print('Error in making cell-data for multi key fields',e)

    return cell_data

def resize_coordinates(box,resize_factor):

    box["width"] = int(box["width"] / resize_factor)
    box["height"] = int(box["height"] / resize_factor)
    box["y"] = int(box["y"] / resize_factor)
    box["x"] = int(box["x"] / resize_factor)

    return box


def get_requied_field_data(field):
    additional_splits = field['additional_splits']
    fields = {'Left': '', 'Right':'', 'Top': '', 'Bottom': ''}
    # dream scenario
    '''
    fields = {'top' : {}, 'left': {}, 'right': {}, 'bottom': {}
    '''
    keyword_and_align = additional_splits['coordinates'][-1]
    coords = additional_splits['coordinates'][:3]

    training_data_field = {}
    coord_counter = 0
    for key,val in keyword_and_align.items():
        training_data_field[val] = {
            'field' : val,
            'keyword' : key,
            'value' : '',
            'validation': {
                'pattern': 'NONE',
                'globalCheck': 'false'
              },
              'split' : 'no',
              'coordinates' : coords[coord_counter],
              'width' : field['width'],
              'page' : coords[coord_counter]['page']
        }
        coord_counter += 1

    for key in fields.keys():
        try:
            fields[key] = training_data_field[key]
        except:
            pass

    return fields

def get_boundary_data(ocr_data, field, resize_factor):
    fields = get_requied_field_data(field)

    field_data = {}
    extracted_data = {}
    junk = ''
    for _, field in fields.items():
        try:
            field_type = field['field']
            keyword = field['keyword']
            field_value = field['value']
            field_box = field['coordinates']
            page_no = field_box['page']
            extracted_data[field_type] = field_value
            try:
                split_check = field['split']
            except:
                split_check = ''
            try:
                validation = field['validation']['pattern']
            except Exception as e:
                print('Validation error',e)
                validation = ''


            # Resize field box
            field_box["width"] = int(field_box["width"] / resize_factor)
            field_box["height"] = int(field_box["height"] / resize_factor)
            field_box["y"] = int(field_box["y"] / resize_factor)
            field_box["x"] = int(field_box["x"] / resize_factor)

            # Scope is field box by default. Keyword box if keyword is there.
            # Its updated later when we check if keyword exists
            scope = {
                'x': field_box['x'],
                'y': field_box['y'],
                'width': field_box['width'],
                'height': field_box['height']
                }
            multi_key_field_info = {}
            context_key_field_info = {}

            try:
                additional_field_info = field['additional_splits']
                if additional_field_info['type'].lower() == '2d':
                    cell_data = get_cell_data(scope,additional_field_info,resize_factor,ocr_data[int(page_no)])
                    multi_key_field_info['cell_data'] = cell_data
                elif additional_field_info['type'].lower() == 'context':
                    context_coords = resize_coordinates(additional_field_info['coordinates'][0],resize_factor)
                    context_scope = {
                                        'x':context_coords['x'],
                                        'y':context_coords['y'],
                                        'width':context_coords['width'],
                                        'height':context_coords['height']
                                    }
                    box = {}
                    box['width'] = context_coords['width']
                    box['height'] = context_coords['height']
                    relative = {
                                'left': scope['x'] - context_scope['x'],
                                'top': scope['y'] - context_scope['y']
                                }
                    # print('context_coords',context_coords)
                    # print('ocr_data type',type(ocr_data))
                    context_ocr_data = ocrDataLocal(context_coords['y'],context_coords['x'],context_coords['x']+context_coords['width'],context_coords['y']+context_coords['height'],ocr_data[int(page_no)])
                    context_text = ' '.join([word['word'] for word in context_ocr_data])
                    context_key_field_info = {
                                                'text': context_text,
                                                'box': box,
                                                'relative':relative
                                            }
                # print('multi_key_field_info',multi_key_field_info)
                # print('context_key_field_info',context_key_field_info)
            except:
                pass
            '''
                Finding keyword using different method
                bcoz I don't trust old method.Hence keyword_box_new
            '''
            haystack = ocrDataLocal(scope['y'],scope['x'],scope['x']+scope['width'],scope['y']+scope['height'],ocr_data[int(page_no)])
            try:
                keyword_box_new = needle_in_a_haystack(keyword,haystack)
            except Exception as e:
                keyword_box_new = {}
                print('Exception in finding keyword:',keyword,'\nError:',e)

            try:
                value_meta = needle_in_a_haystack(field_value,haystack)
            except Exception as e:
                value_meta = {}
                print('Exception in finding keyword',e)


            # Box's Top, Right, Bottom, Left
            box_t = field_box['y']
            box_r = field_box['x'] + field_box['width']
            box_b = field_box['y'] + field_box['height']
            box_l = field_box['x']

            # If keyword is there then save the
            # relative distance from keyword to box's edges
            # else save the box coordinates directly
            if keyword:
                regex = re.compile(r'[@_!#$%^&*()<>?/\|}{~:;]')
                alphareg = re.compile(r'[a-zA-Z]')
                keyList=keyword.split()
                if len(keyList)>1:
                    if regex.search(keyList[-1])!=None and alphareg.search(keyList[-1])==None:
                        #if the last word of keyword sentence containes only special characters
                        junk=keyList[-1]
                        del keyList[-1]
                keyword=' '.join(keyList)
                # Get keyword's box coordinates
                if keyword:
                    keyword_box = keyword_extract(ocr_data[int(page_no)], keyword, scope)


                if not keyword_box:
                    keyword_2, junk = correct_keyword(ocr_data[int(page_no)], keyword, scope ,field_value)
                    keyword_box = keyword_extract(ocr_data[int(page_no)], keyword_2, scope)
                    if keyword_box:
                        field['keyword']=keyword=keyword_2
                if keyword_box:
                    # Keyword's Top, Right, Bottom, Left
                    keyword_t = keyword_box['y']
                    keyword_r = keyword_box['x'] + keyword_box['width']
                    keyword_b = keyword_box['y'] + keyword_box['height']
                    keyword_l = keyword_box['x']

                    # Scope is keyword is keyword exists
                    scope = {
                        'x': keyword_box['x'],
                        'y': keyword_box['y'],
                        'width': keyword_box['width'],
                        'height': keyword_box['height']
                        }


                    # Calculate distance to box edges wrt keyword
                    top = keyword_t - box_t
                    right = box_r - keyword_r
                    bottom = box_b - keyword_b
                    left = keyword_l - box_l
                else:
                    top = box_t
                    right = box_r
                    bottom = box_b
                    left = box_l
            else:
                top = box_t
                right = box_r
                bottom = box_b
                left = box_l

            ''' Storing additional information of value wrt keyword'''
            try:
                field_value_coords_left = {
                                        'top':field_box['y'],
                                        'bottom':field_box['y']+field_box['height'],
                                        'left':value_meta['left'],
                                        'right':value_meta['right']+10
                                    }
                field_value_coords_bottom = {
                                        'top':value_meta['top'],
                                        'bottom':value_meta['bottom'],
                                        'left':field_box['x'],
                                        'right':field_box['x']+field_box['width']
                                    }

                key_val_meta = {}

                if keyword_box_new:
                    print('direction',field_type,get_rel_info(keyword_box_new,value_meta,'direction'))
                    if get_rel_info(keyword_box_new,value_meta,'direction') == 'left':
                        field_value_coords = field_value_coords_left
                    else:
                        field_value_coords = field_value_coords_bottom
                    print('keyword_box_new',keyword_box_new)
                    print('field_value_coords',field_value_coords)
                    key_val_meta = get_rel_info(keyword_box_new,field_value_coords)
                key_val_meta = {**field_value_coords, **key_val_meta}
            except Exception as e:
                key_val_meta = {}
                print('Exception in key val meta',e)
                print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

            # Add to final data
            field_data[field_type] = {
                'keyword': keyword,
                'top': top,
                'right': right,
                'bottom': bottom,
                'left': left,
                'scope': scope,
                'page': page_no,
                'junk' : junk,
                'key_val_meta':key_val_meta,
                'validation':validation,
                'split_check':split_check,
                'multi_key_field_info':multi_key_field_info,
                'context_key_field_info':context_key_field_info,
            }
        except:
            continue
    return field_data

def get_trained_info(ocr_data, fields, resize_factor):
    # Database configuration
    db_config = {
        'host': 'extraction_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    db = DB('extraction', **db_config)
    # db = DB('extraction') # Development purpose

    # ! Getting OCR data from train route
    # get ocr from db
    # ocr_query = "SELECT ocr_data FROM process_queue WHERE file_name= %s "
    # params=[file_name]
    # res= execute_query(ocr_query, ANALYST_DATABASE,params)
    # ocr_data = json.loads(res[0][0])


    # ! Checking if template exists in train route
    # q=("SELECT count(template_name) from trained_info where template_name =%s ")
    # params = [template_name]
    # res=db.execute_query(q,params = params)[0][0]
    # if res>0:
    #     print("Template name is duplicate")
    #     return jsonify({'flag': 'error', 'result': 'Template name already exists in database'})
    print('fields',fields)
    field_data = {}
    extracted_data = {}
    junk = ''
    for _, field in fields.items():
        field_type = field['field']
        keyword = field['keyword']
        field_value = field['value']
        field_box = field['coordinates']
        page_no = field_box['page']
        extracted_data[field_type] = field_value
        try:
            split_check = field['split']
        except:
            split_check = ''
        try:
            validation = field['validation']['pattern']
        except Exception as e:
            print('Validation error',e)
            validation = ''


        # Resize field box
        field_box["width"] = int(field_box["width"] / resize_factor)
        field_box["height"] = int(field_box["height"] / resize_factor)
        field_box["y"] = int(field_box["y"] / resize_factor)
        field_box["x"] = int(field_box["x"] / resize_factor)

        # Scope is field box by default. Keyword box if keyword is there.
        # Its updated later when we check if keyword exists
        scope = {
            'x': field_box['x'],
            'y': field_box['y'],
            'width': field_box['width'],
            'height': field_box['height']
            }
        multi_key_field_info = {}
        context_key_field_info = {}

        try:
            additional_field_info = field['additional_splits']
            if additional_field_info['type'].lower() == '2d':
                cell_data = get_cell_data(scope,additional_field_info,resize_factor,ocr_data[int(page_no)])
                multi_key_field_info['cell_data'] = cell_data
            elif additional_field_info['type'].lower() == 'context':
                context_coords = resize_coordinates(additional_field_info['coordinates'][0],resize_factor)
                context_scope = {
                                    'x':context_coords['x'],
                                    'y':context_coords['y'],
                                    'width':context_coords['width'],
                                    'height':context_coords['height']
                                }
                box = {}
                box['width'] = context_coords['width']
                box['height'] = context_coords['height']
                relative = {
                            'left': scope['x'] - context_scope['x'],
                            'top': scope['y'] - context_scope['y']
                            }
                # print('context_coords',context_coords)
                # print('ocr_data type',type(ocr_data))
                context_ocr_data = ocrDataLocal(context_coords['y'],context_coords['x'],context_coords['x']+context_coords['width'],context_coords['y']+context_coords['height'],ocr_data[int(page_no)])
                context_text = ' '.join([word['word'] for word in context_ocr_data])
                context_key_field_info = {
                                            'text': context_text,
                                            'box': box,
                                            'relative':relative
                                        }
            # print('multi_key_field_info',multi_key_field_info)
            # print('context_key_field_info',context_key_field_info)
        except:
            pass
        '''
            Finding keyword using different method
            bcoz I don't trust old method.Hence keyword_box_new
        '''
        haystack = ocrDataLocal(scope['y'],scope['x'],scope['x']+scope['width'],scope['y']+scope['height'],ocr_data[int(page_no)])
        try:
            keyword_box_new = needle_in_a_haystack(keyword,haystack)
        except Exception as e:
            keyword_box_new = {}
            print('Exception in finding keyword:',keyword,'\nError:',e)

        try:
            value_meta = needle_in_a_haystack(field_value,haystack)
        except Exception as e:
            value_meta = {}
            print('Exception in finding keyword',e)


        # Box's Top, Right, Bottom, Left
        box_t = field_box['y']
        box_r = field_box['x'] + field_box['width']
        box_b = field_box['y'] + field_box['height']
        box_l = field_box['x']

        # If keyword is there then save the
        # relative distance from keyword to box's edges
        # else save the box coordinates directly
        if keyword:
            regex = re.compile(r'[@_!#$%^&*()<>?/\|}{~:;]')
            alphareg = re.compile(r'[a-zA-Z]')
            keyList=keyword.split()
            if len(keyList)>1:
                if regex.search(keyList[-1])!=None and alphareg.search(keyList[-1])==None:
                    #if the last word of keyword sentence containes only special characters
                    junk=keyList[-1]
                    del keyList[-1]
            keyword=' '.join(keyList)
            # Get keyword's box coordinates
            keyword_box = keyword_extract(ocr_data[int(page_no)], keyword, scope)


            if not keyword_box:
                keyword_2, junk = correct_keyword(ocr_data[int(page_no)], keyword, scope ,field_value)
                keyword_box = keyword_extract(ocr_data[int(page_no)], keyword_2, scope)
                if keyword_box:
                    field['keyword']=keyword=keyword_2
            if keyword_box:
                # Keyword's Top, Right, Bottom, Left
                keyword_t = keyword_box['y']
                keyword_r = keyword_box['x'] + keyword_box['width']
                keyword_b = keyword_box['y'] + keyword_box['height']
                keyword_l = keyword_box['x']

                # Scope is keyword is keyword exists
                scope = {
                    'x': keyword_box['x'],
                    'y': keyword_box['y'],
                    'width': keyword_box['width'],
                    'height': keyword_box['height']
                    }


                # Calculate distance to box edges wrt keyword
                top = keyword_t - box_t
                right = box_r - keyword_r
                bottom = box_b - keyword_b
                left = keyword_l - box_l
            else:
                top = box_t
                right = box_r
                bottom = box_b
                left = box_l
        else:
            top = box_t
            right = box_r
            bottom = box_b
            left = box_l

        ''' Storing additional information of value wrt keyword'''
        try:
            field_value_coords_left = {
                                    'top':field_box['y'],
                                    'bottom':field_box['y']+field_box['height'],
                                    'left':value_meta['left'],
                                    'right':value_meta['right']+10
                                }
            field_value_coords_bottom = {
                                    'top':value_meta['top'],
                                    'bottom':value_meta['bottom'],
                                    'left':field_box['x'],
                                    'right':field_box['x']+field_box['width']
                                }

            key_val_meta = {}

            if keyword_box_new:
                print('direction',field_type,get_rel_info(keyword_box_new,value_meta,'direction'))
                if get_rel_info(keyword_box_new,value_meta,'direction') == 'left':
                    field_value_coords = field_value_coords_left
                else:
                    field_value_coords = field_value_coords_bottom
                print('keyword_box_new',keyword_box_new)
                print('field_value_coords',field_value_coords)
                key_val_meta = get_rel_info(keyword_box_new,field_value_coords)
            key_val_meta = {**field_value_coords, **key_val_meta}
        except Exception as e:
            key_val_meta = {}
            print('Exception in key val meta',e)


        # Add to final data
        field_data[field_type] = {
            'keyword': keyword,
            'top': top,
            'right': right,
            'bottom': bottom,
            'left': left,
            'scope': scope,
            'page': page_no,
            'junk' : junk,
            'key_val_meta':key_val_meta,
            'validation':validation,
            'split_check':split_check,
            'multi_key_field_info':multi_key_field_info,
            'context_key_field_info':context_key_field_info,
        }

        try:
            boundary_data = get_boundary_data(ocr_data, field, resize_factor)
            field_data[field_type]['boundary_data'] = boundary_data
        except Exception as e:
            print('Exception in fued method',e)
            #print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))

    # storing fued positional descriptors information
    return field_data


def update_queue_trace(queue_db,case_id,latest):
    queue_trace_q = "SELECT * FROM `trace_info` WHERE `case_id`=%s"
    queue_trace_df = queue_db.execute(queue_trace_q,params=[case_id])

    if queue_trace_df.empty:
        message = f' - No such case ID `{case_id}` in `trace_info`.'
        print(f'ERROR: {message}')
        return {'flag':False,'message':message}
    # Updating Queue Name trace
    try:
        queue_trace = list(queue_trace_df.queue_trace)[0]
    except:
        queue_trace = ''
    if queue_trace:
        queue_trace += ','+latest
    else:
        queue_trace = latest

    #Updating last_updated_time&date

    try:
        last_updated_dates = list(queue_trace_df.last_updated_dates)[0]
    except:
        last_updated_dates = ''
    if last_updated_dates:
        last_updated_dates += ','+ datetime.now().strftime(r'%d/%m/%Y %H:%M:%S')
    else:
        last_updated_dates = datetime.now().strftime(r'%d/%m/%Y %H:%M:%S')

    update = {'queue_trace':queue_trace}
    where = {'case_id':case_id}
    update_q = "UPDATE `trace_info` SET `queue_trace`=%s, `last_updated_dates`=%s WHERE `case_id`=%s"
    queue_db.execute(update_q,params=[queue_trace,last_updated_dates,case_id])

    return {'flag':True,'message':'Updated Queue Trace'}


@app.route('/force_template', methods=['POST', 'GET'])
def force_template():
    ui_data = request.json

    case_id = ui_data['case_id']
    template_name = ui_data['template_name']

    # Database configuration
    db_config = {
        'host': 'queue_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    queue_db = DB('queues', **db_config)

    # Database configuration
    extraction_db_config = {
        'host': 'extraction_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }

    extraction_db = DB('extraction', **extraction_db_config)

    query = "SELECT id, queue from process_queue where case_id = %s"
    current_queue = list(queue_db.execute(query, params=[case_id]).queue)[0]
    if current_queue != 'Template Exceptions':
        return jsonify({'flag': True, 'message': 'Successfully extracted!'})

    fields = {}
    fields['template_name'] = template_name
    fields['cluster'] = None
    fields['queue'] = 'Processing'

    # Insert a new record for each file of the cluster with template name set and cluster removed
    print(f'Extracting for case ID `{case_id}`')

    if retrain == 'yes':
        queue_db.update('process_queue', update=fields, where={'case_id': case_id})
        extraction_db.execute('DELETE from `ocr` where `case_id` = %s', params = [case_id])
        extraction_db.execute('DELETE from `sap` where `case_id` = %s', params = [case_id])
        extraction_db.execute('DELETE from `business_rule` where `case_id` = %s', params = [case_id])

        # Send case ID to extraction topic
        produce('extract', {'case_id': case_id})

        return jsonify({'flag': True, 'message': 'Successfully extracting with new template. Please wait!'})

    cluster_query = "SELECT `id`,`cluster` from `process_queue` where `case_id` = %s"
    cluster = list(queue_db.execute(cluster_query, params=[case_id]).cluster)[0]

    print(cluster, '##############')

    queue_db.update('process_queue', update=fields, where={'case_id': case_id})

    # Send case ID to extraction topic
    produce('extract', {'case_id': case_id})

    if cluster is not None:
        cluster_ids_query = "SELECT id, case_id from `process_queue` where `cluster` = %s and queue = 'Template Exceptions'"
        cluster_case_data = list(queue_db.execute(cluster_ids_query, params=[str(int(cluster))]).case_id)
    else:
        return jsonify({'flag': True, 'message': 'Successfully extracted!'})

    print(cluster_case_data)
    for case_data in cluster_case_data:
        if case_data == case_id:
            print(f'Already extracted for case ID `{case_id}`')
            continue

        cluster_case_id = case_data

        # Update the record for each file of the cluster with template name set and cluster removed
        print(f'Extracting for case ID - Force `{cluster_case_id}`')

        if cluster_case_data != case_id:
            queue_db.update('process_queue', update=fields, where={'case_id': cluster_case_id})

            # Send case ID to extraction topic
            produce('extract', {'case_id': cluster_case_id})


    return jsonify({'flag': True, 'message': 'Successfully extracted!'})

@app.route('/retrain', methods=['POST', 'GET'])
def retrain():
    ui_data = request.json

    # ! Requires `template_name`, `extracted_data`, `case_id`, `trained_data`, `resize_factor`
    # ! `header_ocr`, `footer_ocr`, `address_ocr`
    # Database configuration
    db_config = {
        'host': 'queue_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    queue_db = DB('queues', **db_config)
    # queue_db = DB('queues')

    trained_db_config = {
        'host': 'template_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    trained_db = DB('template_db', **trained_db_config)
    # trained_db = DB('template_db')

    table_db_config = {
        'host': 'table_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    table_db = DB('table_db', **table_db_config)
    # trained_db = DB('template_db')

    extarction_db_config = {
        'host': 'extraction_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    extraction_db = DB('extraction', **extarction_db_config)
    # extraction_db = DB('extraction')

    template_name = ui_data['template_name']
    fields = ui_data['fields']
    case_id = ui_data['case_id']
    table_check = False

    new_vendor = ui_data['temp_type']

    if new_vendor == 'new':
        trained_db.insert_dict({"vendor_name": template_name}, 'vendor_list')

    if json.loads(ui_data['trained_table']):
        try:
            trained_table = json.dumps([[json.loads(ui_data['trained_table'])['0']]])
        except:
            try:
                trained_table = json.dumps([[json.loads(ui_data['trained_table'])['undefined']]])
            except:
                trained_table = '[]'

        table_check = True

    resize_factor = ui_data['resize_factor']

    try:
        table_trained_info = ui_data['table'][0]['table_data']['trained_data']
        table_method = ui_data['table'][0]['method']

        table_data_column_values = {
            'template_name': template_name,
            'method': table_method,
            'table_data': json.dumps(table_trained_info) #bytes(table_trained_info, 'utf-8').decode('utf-8', 'ignore')
        }
        table_db.insert_dict(table_data_column_values, 'table_info')
    except:
        table_trained_info = {}

    # process_queue_df = queue_db.get_all('process_queue')
    query = "SELECT * from process_queue where case_id = %s"
    latest_case = queue_db.execute(query, params=[case_id])


    # * Calculate relative positions and stuff
    query = 'SELECT * FROM `ocr_info` WHERE `case_id`=%s'
    params = [case_id]
    ocr_info = queue_db.execute(query, params=params)
    ocr_data = json.loads(list(ocr_info.ocr_data)[0])
    trained_data = get_trained_info(ocr_data, fields, resize_factor)

    # * Add trained information & template name into `trained_info` table
    trained_data_column_values = {
        'template_name': template_name,
        'field_data': json.dumps(trained_data),
    }
    # trained_db.insert_dict(trained_data_column_values, 'trained_info')
    trained_db.update('trained_info', update=trained_data_column_values, where={'template_name':template_name})
    # TODO: Add table data into a table training microservice database

    # * Save extracted data to ocr table
    # Create a dictionary with field names as key and extracted value as value of the key
    extracted_column_values = {'case_id': case_id}
    columns_in_ocr = extraction_db.get_column_names('ocr')
    extracted_column_values['highlight'] = {}
    for _, field in fields.items():
        column_name = field['field']
        value = field['value']
        value_scope = field['coordinates']
        try:
            page_no = int(field['page'])
        except:
            page_no = 0
        # Check if column name is there in OCR table
        if column_name not in columns_in_ocr:
            print(f' - `{column_name}` does not exist in `ocr` table. Skipping field.')
            continue

        #Add highlight to the dict
        #extracted_column_values['highlight'] = highlight
        highlight = get_highlights(value, ocr_data,value_scope,page_no)
        extracted_column_values['highlight'][column_name] = highlight

        extracted_column_values[column_name] = value

    # highlight = {}
    standardized_data = standardize_date(extracted_column_values)
    standardized_data['highlight'] = json.dumps(extracted_column_values['highlight'])
    if table_check:
        standardized_data['Table'] = trained_table
    extraction_db.update('ocr',standardized_data,{'case_id':case_id})


    return jsonify({'flag': True, 'message': 'Retraining completed!'})



@app.route('/testFields', methods=['POST', 'GET'])
def test_fields():
    data = request.json
    case_id = data['case_id']
    force_check = data['force_check']
    query = "SELECT `id`, `ocr_data` from `ocr_info` WHERE `case_id`=%s"

    db_config = {
        'host': 'queue_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    queue_db = DB('queues', **db_config)

    template_db_config = {
        'host': 'template_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    templates_db = DB('template_db', **template_db_config)

    ocr_data_df = queue_db.execute(query, params=[case_id])

    ocr_data = json.loads(ocr_data_df['ocr_data'].iloc[0])

    if force_check == 'yes':
        template_name = data['template_name']
        trained_info_data = templates_db.get_all('trained_info')
        template_info_df = trained_info_data.loc[trained_info_data['template_name'] == template_name]

        # * Get fields to extract (fte) from the trained info
        trained_info = json.loads(template_info_df.field_data.values[0])
        remove_keys = ['header_ocr', 'footer_ocr', 'address_ocr']
        [trained_info.pop(key, None) for key in remove_keys]
    else:
        field_data = data['field_data']
        resize_factor = data['width']/670
        trained_info = get_trained_info(ocr_data, field_data,resize_factor)


    value_extract_params = {    "case_id":case_id,
                                "field_data":trained_info
                            }

    host = 'servicebridge'
    port = 80
    route = 'predict_field'
    response = requests.post(f'http://{host}:{port}/{route}', json=value_extract_params)
    print('response',response)
    return jsonify({'flag':'true', 'data':response.json()})

@app.route('/train', methods=['POST', 'GET'])
def train():
    ui_data = request.json

    # ! Requires `template_name`, `extracted_data`, `case_id`, `trained_data`, `resize_factor`
    # ! `header_ocr`, `footer_ocr`, `address_ocr`
    print('ui_data',ui_data)
    # Database configuration
    db_config = {
        'host': 'queue_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    queue_db = DB('queues', **db_config)
    # queue_db = DB('queues')

    trained_db_config = {
        'host': 'template_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    trained_db = DB('template_db', **trained_db_config)
    # trained_db = DB('template_db')

    table_db_config = {
        'host': 'table_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    table_db = DB('table_db', **table_db_config)
    # trained_db = DB('template_db')

    extarction_db_config = {
        'host': 'extraction_db',
        'user': 'root',
        'password': 'root',
        'port': '3306'
    }
    extraction_db = DB('extraction', **extarction_db_config)
    # extraction_db = DB('extraction')

    template_name = ui_data['template_name']
    new_vendor = ui_data['temp_type']
    fields = ui_data['fields']
    case_id = ui_data['case_id']

    if new_vendor == 'new':
        trained_db.insert_dict({"vendor_name": template_name}, 'vendor_list')

    try:
        trained_table = json.dumps([[json.loads(ui_data['trained_table'])['0']]])
    except:
        try:
            trained_table = json.dumps([[json.loads(ui_data['trained_table'])['undefined']]])
        except:
            trained_table = '[]'
    resize_factor = ui_data['resize_factor']
    header_ocr = ui_data['template']['header_ocr']['value']
    footer_ocr = ui_data['template']['footer_ocr']['value']
    address_ocr = [ui_data['template']['address_ocr']['value']] # A list because ... idk ask Ashish
    # * Check if template name already exists
    trained_info = trained_db.get_all('trained_info')
    trained_template_names = list(trained_info.template_name)
    if template_name.lower() in [t.lower() for t in trained_template_names]:
        message = f'Template name `{template_name}` already exist.'
        print(message)
        return jsonify({'flag': False, 'message': message})
    try:
        table_trained_info = ui_data['table'][0]['table_data']['trained_data']
        table_method = ui_data['table'][0]['method']

        table_data_column_values = {
            'template_name': template_name,
            'method': table_method,
            'table_data': json.dumps(table_trained_info) #bytes(table_trained_info, 'utf-8').decode('utf-8', 'ignore')
        }
        table_db.insert_dict(table_data_column_values, 'table_info')
    except:
        table_trained_info = {}

    # process_queue_df = queue_db.get_all('process_queue')
    # latest_process_queue = queue_db.get_latest(process_queue_df, 'case_id', 'created_date')
    query = "SELECT * from process_queue where case_id = %s"
    latest_case = queue_db.execute(query,params=[case_id])


    # * Calculate relative positions and stuff
    query = 'SELECT * FROM `ocr_info` WHERE `case_id`=%s'
    params = [case_id]
    ocr_info = queue_db.execute(query, params=params)
    ocr_data = json.loads(list(ocr_info.ocr_data)[0])
    trained_data = get_trained_info(ocr_data, fields, resize_factor)

    # * Add trained information & template name into `trained_info` table
    trained_data_column_values = {
        'template_name': template_name,
        'field_data': json.dumps(trained_data),
        'header_ocr': header_ocr,
        'footer_ocr': footer_ocr,
        'address_ocr': json.dumps(address_ocr)
    }
    trained_db.insert_dict(trained_data_column_values, 'trained_info')

    # TODO: Add table data into a table training microservice database

    # * Save extracted data to ocr table
    # Create a dictionary with field names as key and extracted value as value of the key
    extracted_column_values = {'case_id': case_id}
    columns_in_ocr = extraction_db.get_column_names('ocr')
    extracted_column_values['highlight'] = {}
    for _, field in fields.items():
        column_name = field['field']
        value = field['value']
        value_scope = field['coordinates']
        try:
            page_no = int(field['page'])
        except:
            page_no = 0
        # Check if column name is there in OCR table
        if column_name not in columns_in_ocr:
            print(f' - `{column_name}` does not exist in `ocr` table. Skipping field.')
            continue

        #Add highlight to the dict
        #extracted_column_values['highlight'] = highlight
        highlight = get_highlights(value, ocr_data,value_scope,page_no)
        extracted_column_values['highlight'][column_name] = highlight

        extracted_column_values[column_name] = value

    # highlight = {}
    standardized_data = standardize_date(extracted_column_values)
    standardized_data['highlight'] = json.dumps(extracted_column_values['highlight'])
    standardized_data['Table'] = trained_table
    standardized_data['Vendor Name'] = template_name

    extraction_db.insert_dict(standardized_data, 'ocr')


    # * Update the queue name and template name in the process_queue
    update = {'queue':'Verify','template_name':template_name}
    where = {'case_id':case_id}
    queue_db.update('process_queue',update=update,where=where)

    #updating queue trace information
    update_queue_trace(queue_db,case_id,'Verify')

    # Send case ID to extraction topic
    produce('business_rules', {'stage': 'One', 'case_id': case_id, 'send_to_topic': 'sap'})

    # # To Enable only training
    # return jsonify({'flag': True, 'message': 'Training completed!'})


    # * Run extraction on the same cluster
    cluster = list(latest_case.cluster)[0]

    query = "SELECT * from process_queue where cluster = %s and queue = 'Template Exceptions'"
    process_queue_df = queue_db.execute(query,params=[cluster])
    cluster_files_df = process_queue_df.loc[process_queue_df['cluster'] == cluster]
    cluster_case_data = cluster_files_df.to_dict(orient='records')
    for case_data in cluster_case_data:
        if case_data['case_id'] == case_id:
            print(f'Already extracted for case ID `{case_id}`')
            continue

        cluster_case_id = case_data['case_id']

        # Update the record for each file of the cluster with template name set and cluster removed
        print(f'Extracting for case ID - cluster `{cluster_case_id}`')

        fields['template_name'] = template_name
        fields['cluster'] = None

        queue_db.update('process_queue', update=fields, where={'case_id': cluster_case_id})

        # Send case ID to extraction topic
        produce('extract', {'case_id': cluster_case_id})

    return jsonify({'flag': True, 'message': 'Training completed!'})

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port Number', default=5019)
    parser.add_argument('--host', type=str, help='Host', default='0.0.0.0')

    args = parser.parse_args()

    host = args.host
    port = args.port

    app.run(host=host, port=port, debug=False)
