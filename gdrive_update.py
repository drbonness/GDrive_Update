import os, shutil  
import subprocess, re # command-line expressions, regular expressions
import sqlite3  # sqlite, .db commands
import time 
import hashlib  # md5 checksums
from tkinter import *
from tkinter import ttk

# TKinter ------------------------------------------------------------------------------------

def choosePath(path_num):
    path = tkFileDialog.askdirectory()
    if(path is not ""):
        updatePath(path_num, path)
        writeCSV([dir_location])
        
def updatePath(path_num, path):
    dir_location[path_num] = path
    path_display[path_num].set(shortenPath(path))

# GENERIC ------------------------------------------------------------------------------------

# print a status line that will clear the terminal output before printing
def print_statusline(msg: str):
    last_msg_length = len(print_statusline.last_msg) if hasattr(print_statusline, 'last_msg') else 0
    print(' ' * last_msg_length, end='\r')
    print(msg, end='\r')
    print_statusline.last_msg = msg
    
# md5 checksum, read with chunks of 4096 bytes 
def md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# parse a line of delimeted input and return last item
def get_last(cl_output,delimeter):
    cl_separated = re.findall(str('[^'+delimeter+']+'),cl_output)
    return cl_separated[len(cl_separated)-1]

# create question mark sytax (?,?,?,...) for specified number of columns in table
def create_qmarks(columns):
    qmark_string = '('
    
    for q in range(0,columns):
        qmark_string = qmark_string + '?'
        if q < (columns - 1):
            qmark_string = qmark_string + ','
            
    qmark_string = qmark_string + ')'
    return qmark_string

# get the volume path from a file_path
def get_volume_path (file_path):
    file_path_split = re.findall('/[^/]*', file_path)
    volume_path = ''
    if file_path_split[0] == '/Volumes':
        volume_path = file_path_split[0]+file_path_split[1]
    else:
        volume_path = '/'
    return volume_path

# returns cloud_entry, local_entry, mapping, volume_info
def get_volume_uuid (drive_path):
    
    volume_path = get_volume_path(drive_path)   # get root volume path
    p_info = subprocess.check_output(['diskutil','info',str(volume_path)]).decode("utf-8")  # open diskutil process using volume path
    
    volume_field = 'Volume UUID'    # header tag for finding volume uuid in diskutil info
    
    line = re.search(str(volume_field + '.*'),p_info)[0] # get info from diskutil based on header tag
    volume_uuid = get_last(line,':').strip()    # parse text to extract uuid
    return volume_uuid

# copy file to destination
def copy_file (file_path, dest_path):
    file_name = get_last(file_path, '\/')
    shutil.copyfile(file_path, os.path.join(dest_path, file_name))

# delete existing files at path unless they are supposed to be ignored
# (allows adding more folders to ignore)
def delete_path_files(path, ignore):
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        try:
            if not any(i in file for i in ignore):
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path): 
                    shutil.rmtree(file_path)
        except Exception as e:
            print('Error: ' + str(e))

# close a process - kill_immediate determines if it should kill the process or send it an exit code
def close_process(proc_name, kill_immediate):
    kill_flag = '3'
    if(kill_immediate):
        kill_flag = '9'
    try:
        os.system('killall -' + kill_flag + ' "' + proc_name + '" 2>/dev/null')
        print('Process terminated: ' + proc_name)
    except Exception as e:
        print('Error: ' + str(e))
        
# write to CSV file
def writeCSV(output_list, config_filename):
    file = open(config_filename,"w")
    for x in range(len(output_list)):
        file.seek(0);
        csv_writer = csv.writer(file, delimiter=',', quotechar='"')
        csv_writer.writerow(output_list[x])

            
# SYNC_CONFIG.DB ------------------------------------------------------------------------------------

# update sync_config.db to reflect new Google Drive path
def update_sync_config(config_path, drive_path):

    config = sqlite3.connect(config_path)
    c_con = config.cursor()
        
    d_change = ['local_sync_root_path','root_config__0','root_config__1','root_config__2']

    c_con.execute("UPDATE data SET data_value = ? WHERE entry_key = ?", (drive_path, d_change[0]))
    c_con.execute("UPDATE data SET data_value = ? WHERE entry_key = ?", (drive_path, d_change[1]))
    c_con.execute("UPDATE data SET data_key = ? WHERE entry_key = ?", (drive_path, d_change[2]))
    c_con.execute("UPDATE data SET data_key = ? WHERE entry_key = ?", (drive_path, d_change[3]))
    config.commit()

    c_con.close
    config.close
    
# SNAPSHOT.DB ------------------------------------------------------------------------------------

# create snapshot.db
def create_snapshot(snapshot_path):

    snapshot = sqlite3.connect(snapshot_path)
    c_sn = snapshot.cursor()

    c_sn.execute('CREATE TABLE cloud_entry (doc_id TEXT NOT NULL, filename TEXT, modified INTEGER, created INTEGER, acl_role INTEGER, doc_type INTEGER, removed INTEGER, size INTEGER, checksum TEXT, shared INTEGER, resource_type TEXT, original_size INTEGER, original_checksum TEXT, down_sample_status INTEGER, PRIMARY KEY (doc_id))')
    c_sn.execute('CREATE TABLE cloud_relations (child_doc_id TEXT NOT NULL, parent_doc_id TEXT NOT NULL, UNIQUE (child_doc_id, parent_doc_id), FOREIGN KEY (child_doc_id) REFERENCES cloud_entry(doc_id) ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE, FOREIGN KEY (parent_doc_id) REFERENCES cloud_entry(doc_id) ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE)')
    c_sn.execute('CREATE TABLE local_entry (inode INTEGER NOT NULL, volume TEXT NOT NULL, filename TEXT, modified INTEGER, checksum TEXT, size INTEGER, is_folder INTEGER, PRIMARY KEY (inode, volume))')
    c_sn.execute('CREATE TABLE local_relations (child_inode INTEGER NOT NULL, child_volume TEXT NOT NULL, parent_inode INTEGER NOT NULL, parent_volume TEXT NOT NULL, UNIQUE (child_inode, child_volume), FOREIGN KEY (parent_inode, parent_volume) REFERENCES local_entry(inode, volume) ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE, FOREIGN KEY (child_inode, child_volume) REFERENCES local_entry(inode, volume) ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE)')
    c_sn.execute('CREATE TABLE mapping (inode INTEGER NOT NULL, volume TEXT NOT NULL, doc_id TEXT NOT NULL, UNIQUE (inode, volume), FOREIGN KEY (inode, volume) REFERENCES local_entry(inode, volume) ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE, FOREIGN KEY (doc_id) REFERENCES cloud_entry(doc_id) ON DELETE NO ACTION ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE)')
    c_sn.execute('CREATE TABLE pre_mapping (inode INTEGER NOT NULL, volume TEXT NOT NULL, path TEXT, doc_id TEXT NOT NULL, UNIQUE (inode, volume))')
    c_sn.execute('CREATE TABLE volume_info (volume TEXT NOT NULL, full_path TEXT, uuid TEXT, label TEXT, size INTEGER, filesystem TEXT, model TEXT, device_type TEXT, device_file TEXT, device_number INTEGER, PRIMARY KEY (volume))')

    c_sn.execute('CREATE INDEX cloud_entry_down_sample_status_idx ON cloud_entry (down_sample_status)')
    c_sn.execute('CREATE INDEX cloud_relations_child_doc_id_idx on cloud_relations (child_doc_id)')
    c_sn.execute('CREATE INDEX cloud_relations_parent_doc_id_idx on cloud_relations (parent_doc_id)')
    c_sn.execute('CREATE INDEX local_relations_parent_inode_parent_volume_idx on local_relations (parent_inode, parent_volume)')
    c_sn.execute('CREATE INDEX mapping_doc_id_idx on mapping (doc_id)')
    c_sn.execute('CREATE INDEX mapping_inode_volume_idx on mapping (inode, volume)')
    c_sn.execute('CREATE INDEX pre_mapping_doc_id_idx on pre_mapping (doc_id)')
    c_sn.execute('CREATE INDEX pre_mapping_inode_volume_idx on pre_mapping (inode, volume)')
    snapshot.commit()
    c_sn.close
    snapshot.close
    
def new_snapshot(snapshot_path):
    try:
        if os.path.isfile(snapshot_path):   # if snapshot.db already exists at snapshot_path
            os.unlink(snapshot_path)        # delete it
    except:
        print('Error: Failed to delete previous snapshot.db file')
        
    try:
        if not os.path.isfile(snapshot_path):   # if snapshot.db does not exist at snapshot_path
            create_snapshot(snapshot_path)      # create it
    except:
        print('Error: Failed to create new snapshot.db file')

#initialiaze snapshot.db with appropriate values
def initialize_snapshot(snapshot_path, drive_path):
    
    snapshot_was_synced = False
    
    if not os.path.isfile(snapshot_path):   # if snapshot.db does not exist at snapshot_path
        create_snapshot(snapshot_path)      # create it
    
    snapshot = sqlite3.connect(snapshot_path)
    c_sn = snapshot.cursor()
    
    cloud_entry, local_entry, mapping, volume_info = snapshot_init_info(drive_path)
    
    cloud_entry_columns = '(doc_id, filename, modified, created, acl_role, doc_type, removed, size, checksum, shared, resource_type, original_size, original_checksum, down_sample_status)'
    cloud_relations_columns = '(child_doc_id, parent_doc_id)'
    local_entry_columns = '(inode, volume, filename, modified, checksum, size, is_folder)'
    local_relations_columns = '(child_inode, child_volume, parent_inode, parent_volume)'
    mapping_columns = '(inode, volume, doc_id)'
    pre_mapping_columns = '(inode, volume, path, doc_id)'
    volume_info_columns = '(volume, full_path, uuid, label, size, filesystem, model, device_type, device_file, device_number)'
    
    cloud_entry_init_columns = '(doc_id, filename, doc_type, shared)'
    local_entry_init_columns = '(inode, volume, filename, is_folder)'
    
    snapshot_root_info = c_sn.execute('SELECT inode FROM mapping WHERE doc_id = ?',['root']) # find root inode of current snapshot.db
    root_inode = snapshot_root_info.fetchone()
    
    if(root_inode == None): # if snapshot.db has not already been initialized, create data
        c_sn.execute('INSERT or REPLACE into local_entry' + local_entry_init_columns + 'VALUES' + create_qmarks(len(local_entry)), local_entry)
        c_sn.execute('INSERT or REPLACE into mapping' + mapping_columns + 'VALUES'+create_qmarks(len(mapping)), mapping)
    else:   # if snapshot.db has already been initialized, update data
        snapshot_was_synced = True;
        c_sn.execute('UPDATE local_entry SET' + local_entry_init_columns +  '=' + create_qmarks(len(local_entry)) + 'WHERE inode = ?', local_entry + [root_inode[0]])
        c_sn.execute('UPDATE mapping SET' + mapping_columns +  '=' + create_qmarks(len(mapping)) + 'WHERE doc_id = ?', mapping + ['root'])
        c_sn.execute('UPDATE local_relations SET parent_inode = ? WHERE parent_inode = ?', [local_entry[0],root_inode[0]])
                     
    c_sn.execute('INSERT or REPLACE into cloud_entry' + cloud_entry_init_columns + 'VALUES' + create_qmarks(len(local_entry)), cloud_entry)
    c_sn.execute('INSERT or REPLACE into volume_info' + volume_info_columns + 'VALUES'+create_qmarks(len(volume_info)), volume_info)
    
    snapshot.commit()

    c_sn.close
    snapshot.close
    
    return snapshot_was_synced

# update old snapshot values (inodes, volume) with new values (new drive path)
def update_snapshot (snapshot_path, cloud_path, drive_path):
    
    success = False # Did the program successfully update snapshot.db?
    sync_path_exists = os.path.exists(drive_path)
    
    if(not sync_path_exists):
        try:
            os.makedirs(drive_path)    # make it
            new_snapshot(snapshot_path) # create a new snapshot.db file
        except:
            print('Error: Failed to create sync_directory / make new snapshot.db file')
            
    if(os.path.exists(drive_path)):
        
        snapshot_was_synced = initialize_snapshot(snapshot_path,drive_path)
        success = True  # snapshot.db file was successfully initialized

        if(snapshot_was_synced and sync_path_exists):
            success, inode_map = map_inodes(snapshot_path, drive_path)  # map previous inode to current inode and save to list

            snapshot = sqlite3.connect(snapshot_path)
            c_sn = snapshot.cursor()
            
            volume_uuid = get_volume_uuid(drive_path)   # get the drive_path volume uuid

            for inode_pair in inode_map:    # iterate through every inode pair in the inode_map list and update inodes
                flipped_inode_pair = [inode_pair[1],inode_pair[0]] # put the original inode at the end for sql update
                c_sn.execute('UPDATE local_entry SET inode = ? WHERE inode = ?', flipped_inode_pair)
                c_sn.execute('UPDATE local_relations SET child_inode = ? WHERE child_inode = ?', flipped_inode_pair)
                c_sn.execute('UPDATE local_relations SET parent_inode = ? WHERE parent_inode = ?', flipped_inode_pair)
                c_sn.execute('UPDATE local_relations SET (child_volume, parent_volume) = ' + create_qmarks(2), (volume_uuid,volume_uuid))
                c_sn.execute('UPDATE mapping SET inode = ? WHERE inode = ?', flipped_inode_pair)
                c_sn.execute('UPDATE mapping SET volume = ?', [volume_uuid])

            snapshot.commit()

            c_sn.close
            snapshot.close
        
        if(sync_path_exists):   # if the sync path (drive_path) already existed
            success = generate_snapshot (snapshot_path, cloud_path, drive_path) # generate snapshot.db from contents in drive_path
        
    return success
            
def generate_snapshot (snapshot_path, cloud_path, drive_path):

        success, list_length, cloud_entry, local_entry, cloud_relations, local_relations, mapping = generate_snapshot_info(cloud_path, drive_path)

        if(success):    # if the snapshot info was generated successfuly
            
            cloud_entry_columns = '(doc_id, filename, modified, created, acl_role, doc_type, removed, size, checksum, shared, resource_type, original_size, original_checksum, down_sample_status)'
            cloud_relations_columns = '(child_doc_id, parent_doc_id)'
            local_entry_columns = '(inode, volume, filename, modified, checksum, size, is_folder)'
            local_relations_columns = '(child_inode, child_volume, parent_inode, parent_volume)'
            mapping_columns = '(inode, volume, doc_id)'
            
            snapshot = sqlite3.connect(snapshot_path)
            c_sn = snapshot.cursor()

            for i in range(0,list_length):  # iterate through every row in the generated lists and add info to snapshot.db tables
                c_sn.execute('INSERT or REPLACE into cloud_entry' + cloud_entry_columns + 'VALUES' + create_qmarks(len(cloud_entry[i])), cloud_entry[i])
                c_sn.execute('INSERT or REPLACE into local_entry' + local_entry_columns + 'VALUES'+ create_qmarks(len(local_entry[i])), local_entry[i])
                c_sn.execute('INSERT or REPLACE into cloud_relations' + cloud_relations_columns + 'VALUES'+create_qmarks(len(cloud_relations[i])), cloud_relations[i])
                c_sn.execute('INSERT or REPLACE into local_relations' + local_relations_columns + 'VALUES' + create_qmarks(len(local_relations[i])), local_relations[i])
                c_sn.execute('INSERT or REPLACE into mapping' + mapping_columns + 'VALUES'+create_qmarks(len(mapping[i])), mapping[i])

            snapshot.commit()
            c_sn.close
            snapshot.close
            
        else:
            print('Google Drive sync attempt failed')
            
        return success
        
            
# SNAPSHOT.DB FUNCTIONS ------------------------------------------------------------------------------------
            
# returns cloud_entry, local_entry, mapping, volume_info
def snapshot_init_info (drive_path):
    
    cloud_entry = ['root','root','0','0']
    
    volume_path = get_volume_path(drive_path)
    p_info = subprocess.check_output(['diskutil','info',str(volume_path)]).decode("utf-8")

    # diskutil header tags
    # 0 - volume, uuid; 'Volume UUID'
    # 1 - full_path; 'Mount Point'
    # 2 - label; 'Volume Name'
    # 3 - size; 'Disk Size'
    # 4 - filesystem; 'Type \(Bundle\)'
    # 5 - model; 'Device / Media Name'
    # 6 - device_type; 'Device Location'
    # 7 - device_file; volume_path
    # 8 - device_number; os.stat(volume_info[1]).st_dev

    # get volume_info using header tags
    volume_fields = ['Volume UUID','Mount Point','Volume Name','Disk Size','Type \(Bundle\)','Part of Whole','Device Location','Device Identifier']
    volume_info = []

    # find every piece of info from diskutil based on header tag
    for f in range(0,len(volume_fields)):
        line = re.search(str(volume_fields[f] + '.*'),p_info)[0]
        if f == 3:
            cl_separated = re.findall('(?<=\().[^\s]*',line)
            volume_info.append(cl_separated[0])
        elif f == 4:
            volume_info.append(get_last(line,':').lower().strip())
        else:
            volume_info.append(get_last(line,':').strip())

    # append device_number to volume_info
    volume_info.append(os.stat(volume_info[1]).st_dev)
    
    # get model
    p_root = subprocess.check_output(['diskutil','info',volume_info[5]]).decode("utf-8")
    volume_line = re.search(str('Device / Media Name.*'),p_root)[0]
    volume_info[5] = get_last(volume_line,':').strip()

    # format for snapshot.db table "volume_info"
    volume_info.insert(2,volume_info[0])
    
    drive_path_inode = os.stat(drive_path).st_ino
    local_entry = [drive_path_inode,volume_info[0],drive_path,'1']
    mapping = [drive_path_inode,volume_info[0],'root']
    
    return cloud_entry, local_entry, mapping, volume_info

# map previous inode to current inode
def map_inodes (snapshot_path, drive_path):
    
    success = True # Did the program successfully generate an list of inode pairs?
    
    filename_list = [] # list of all filenames [0] & full paths [1] in folder
    inode_map = []  # list of inode pairs for files that have moved (current inode, previous inode)
    
    local_entry_columns = '(inode, volume, filename, modified, checksum, size, is_folder)'
    
    # walk through the drive_path tree and get names and paths of all files and folders present
    for subdir, dirs, files in os.walk(drive_path):
        files_to_ignore = ['Icon','.DS_Store']
        folders_to_ignore = ['.tmp.drivedownload']
        for file in files:
            if not any(i in file for i in files_to_ignore):
                filename_list.append([file,os.path.join(subdir,file)])
        for d in dirs:
            if not (d == [] or any(i in d for i in folders_to_ignore)):
                filename_list.append([d,os.path.join(subdir,d)])

    snapshot = sqlite3.connect(snapshot_path)
    c_sn = snapshot.cursor()
    
    # for every path in path_list, check if it exists in local_entry
    # to ensure it is the same file / folder, check modification date, size, checksum, & whether it is a file or folder
    for file in filename_list:

        synced_snapshot_files = c_sn.execute('SELECT inode,modified,checksum,size,is_folder FROM local_entry WHERE filename = ?',[file[0]])
        counter = 0 # reset counter - folders cannot be rigorously matched; if two folders have the same name, there will be a matching error

        for s in synced_snapshot_files:
            
            file_stats = os.stat(file[1])
            is_folder = os.path.isdir(file[1])*1    # is the file a folder?
            compare_info = [file_stats.st_ino,file_stats.st_mtime]     # get file inode & modification date
            
            if is_folder:   # if the files is a folder
                if(counter==0 and s[4] == 1):   # if it is the first matched folder and is_folder in local_entry == 1 (True)
                    #compare_info.extend([None,None,is_folder]) # not necessary, no attributes can be compared
                    inode_map.append([s[0],compare_info[0]])    # append inode pair (current inode, previous inode)
                else:
                    success = False # there was an error
                    print('Error: Multiple folders have the same name in the original directory')
            elif md5_check: # if the file is not a folder and the md5 checksum should be compared
                md5_checksum = md5(file[1])
                compare_info.extend([md5_checksum,file_stats.st_size,is_folder])
                if all(compare_info[c] == s[c] for c in [1,2,3,4]): # check modification date, md5_checksum, size, and is_folder
                    inode_map.append([s[0],compare_info[0]])        # append inode pair (current inode, previous inode)
            else:   # if the file is not a folder and the md5 checksum should not be compared
                compare_info.extend([None,file_stats.st_size,is_folder])
                if all(compare_info[c] == s[c] for c in [1,3,4]):   # check modification date, size, and is_folder
                    inode_map.append([s[0],compare_info[0]])        # append inode pair (current inode, previous inode)

            counter += 1    # increment counter

    c_sn.close
    snapshot.close
    
    return success, inode_map

def generate_snapshot_info(cloud_path, drive_path):
    
    success = False # Did the program successfully generate all snapshot.db info?
    
    # Note - resource_type is missing because it does not exist in cloud_graph entry. Set resource_type = Null
    # 0 - doc_id
    # 1 - filename
    # 2 - modified
    # 3 - created
    # 4 - acl_role
    # 5 - doc_type
    # 6 - removed
    # 7 - size
    # 8 - checksum
    # 9 - shared
    # 10 - original_size
    
    # 10 - INSERT resource_type here - Null (None)
    
    # 11 - original_checksum
    # 12 - down_sample_status
    cloud_entry_columns = 'doc_id, filename, modified, created, acl_role, doc_type, removed, size, checksum, shared, original_size, original_checksum, down_sample_status'
    
    cloud_entry = []    # list for storing cloud_entry info
    local_entry = []    # list for storing local_entry info
    cloud_relations = []    # list for storing cloud_relations info
    local_relations = []    # list for storing local_relations info
    mapping = []    # list for storing mapping info
    pre_mapping = []    # list for storing pre_mapping info
    
    volume_uuid = get_volume_uuid(drive_path)
    
    drive_extensions = ['.gsheet','.gdoc','.gslides','.gdraw','.gtable','.gform']   # extension types for Google Docs file
    
    cloud = sqlite3.connect(cloud_path) # connect to cloud_graph.db
    c_c = cloud.cursor()
    
    top_level = True;   # Is this the top_level of the drive_path tree
    
    parent_doc_id_dict = {} # dictionary for retrieving doc_id of current subdir. Use doc_id as parent_doc_id to retrieve child_doc_id's
    subdir_count = 0    # counter to determine how many subdirectories have been completed  - progress output
    file_count = 0    # counter to determine how many files / folders have been examined - progress output
    
    # walk through the drive_path tree
    for subdir, dirs, files in os.walk(drive_path):
        files_to_ignore = ['Icon','.DS_Store']
        folders_to_ignore = ['.tmp.drivedownload']
        child_doc_ids = []  # list of all child_doc_id's in the current subdir
        
        last_msg_length = len(print_statusline.last_msg) if hasattr(print_statusline, 'last_msg') else 0
        print(' ' * last_msg_length, end='\r')
        print_statusline('Subdirectories Completed: {0:3}; Files / Folders Examined: {1:3}; Current Folder: {2}'.format(subdir_count,file_count,get_last(subdir,'/')))
        
        if(not any(i in subdir for i in folders_to_ignore)):
            if(top_level):  # If this is the top_level of the drive_path tree
                parent_doc_id_dict[subdir] = ['root']   # update parent_doc_id dictionary by adding the current directory path & doc_id
                top_level = False   # No longer at the top_level after this iteration
            try:
                cloud_child_ids = c_c.execute('SELECT child_doc_id FROM cloud_relations WHERE parent_doc_id = ?',parent_doc_id_dict[subdir]) # find child ids at current subdir
                child_doc_ids = [child_doc_id[0] for child_doc_id in cloud_child_ids.fetchall()]    # fetch results
            except Exception as e:
                print('Subdirectory was not matched to folder in cloud_graph.db\nKey Error:' + str(e))
        
        if(len(child_doc_ids) > 0):
           
            parent_inode = os.stat(subdir).st_ino
           
            for file in files:  # for every file in the current subdir
                if not any(i in file for i in files_to_ignore): # check to see if the file should be ignored

                    gdoc_file = False   # Is it a Google Docs file?
                    file_path = os.path.join(subdir,file)   # full path of the file being examined

                    for extension in drive_extensions:  # for all Google Docs extension types
                        if extension in file:   # if an extension is a part of the filename
                            file = file.replace(extension,'')   # remove it from the filename
                            gdoc_file = True    # this is a Google Docs file

                    cloud_files = c_c.execute('SELECT ' + cloud_entry_columns + ' FROM cloud_graph_entry WHERE filename = ?',[file])    # search for file by filename in cloud_graph_entry table of cloud_graph.db

                    for c in cloud_files:   # for every returned result of the sqlite query
                        
                        if(c[0] in child_doc_ids and c[5] > 0): # if the current doc_id is contained in child_doc_ids and filetype is a file (c[5] > 0)

                            matched = False;    # Does the current file match a file in Google Drive?

                            file_stats = os.stat(file_path)
                            compare_file_info = [file_stats.st_mtime, file_stats.st_size]    # get file modification date & file size
                            compare_cloud_info = [c[2],c[7]]    # put cloud modification date & file size in a list

                            if(not gdoc_file):  # if the file is not a Google Docs file
                                if md5_check:
                                    md5_checksum = md5(file_path)
                                    compare_file_info.extend([md5_checksum])
                                    compare_cloud_info.extend([c[8]])

                                    if (compare_file_info == compare_cloud_info):   # Compare midification date, file size, & md5 checksum
                                        matched = True
                                else:
                                    if (compare_file_info == compare_cloud_info):   # Compare midification date & file size
                                        matched = True
                            else:   # if the file is a Google Docs file
                                if (compare_file_info[0] == compare_cloud_info[0]): # Compare modification date
                                    matched = True

                            if(matched):    # if the current file matches a file in Google Drive
                                child_inode = file_stats.st_ino     # child_inode = inode of current file
                                child_id = c[0]     # child_doc_id = current doc_id
                                parent_id = parent_doc_id_dict[subdir][0]   # parent_id = doc_id of current subdirectory
                                
                                # append information of current file to all snapshot.db table lists - see generate_snapshot for table columns
                                current_cloud_entry = list(c)  # current row (file) in cloud_files
                                current_cloud_entry.insert(10, None)  # insert None as resource_type
                                cloud_entry.append(current_cloud_entry)  # append current cloud entry information to cloud_entry
                                # current local entry information; cloud indices in local_entry - filename, modified, checksum, size; is_folder = 0
                                current_local_entry = [child_inode, volume_uuid] + [current_cloud_entry[i] for i in [1,2,8,7]] + [0]
                                
                                if(gdoc_file):  # if the file is a Google Docs file
                                    # the cloud_entry table has no md5_checksum nor size information
                                    # md5_checksum and size must be generated (not pulled from the cloud) - see generate_snapshot for table columns
                                    md5_checksum = md5(file_path)
                                    current_local_entry[4] = md5_checksum
                                    current_local_entry[5] = file_stats.st_size
                                
                                local_entry.append(current_local_entry)
                                cloud_relations.append([child_id, parent_id])
                                local_relations.append([child_inode, volume_uuid, parent_inode, volume_uuid])
                                mapping.append([child_inode,volume_uuid,child_id])
                                
                    file_count += 1

            for d in dirs:  # for every directory in current subdirectory
                if not (d == [] or any(i in d for i in folders_to_ignore)):
                    dir_path = os.path.join(subdir,d)

                    cloud_files = c_c.execute('SELECT ' + cloud_entry_columns + ' FROM cloud_graph_entry WHERE filename = ?',[d])    # search for dir by name in cloud_graph_entry table of cloud_graph.db
                    
                    # folders cannot be perfectly matched; if two folders have the same name in
                    # the same Google Drive subdirectory, there will be a matching error
                    counter = 0 # reset counter

                    for c in cloud_files:   # for every returned result of the sqlite query
                        
                        if(c[0] in child_doc_ids and c[5] == 0): # if the current doc_id is contained in child_doc_ids and filetype is a folder (c[5] == 0)
                            if(counter == 0):
                                parent_doc_id_dict[dir_path] = [c[0]]   # update parent_doc_id dictionary by adding the current directory path & doc_id
                                
                                child_inode = os.stat(dir_path).st_ino     # child_inode = inode of current file
                                child_id = c[0]     # child_doc_id = current doc_id
                                parent_id = parent_doc_id_dict[subdir][0]   # parent_id = doc_id of current subdirectory
                                
                                # append information of current file to all snapshot.db table lists - see generate_snapshot for table columns
                                current_cloud_entry = list(c)  # current row (file) in cloud_files
                                current_cloud_entry.insert(10, None)  # insert None as resource_type
                                cloud_entry.append(current_cloud_entry)  # append current cloud entry information to cloud_entry
                                # cloud indices in local_entry - filename, modified, checksum, size; is_folder = 1
                                local_entry.append([child_inode, volume_uuid] + [current_cloud_entry[i] for i in [1,2,8,7]] + [1])
                                cloud_relations.append([child_id, parent_id])
                                local_relations.append([child_inode, volume_uuid, parent_inode, volume_uuid])
                                mapping.append([child_inode,volume_uuid,child_id])
                                
                                counter += 1    # increment counter
                            else:
                                print('Error: Multiple folders in the same Google Drive directory have the same name')
                    
                    file_count += 1
                                
        subdir_count += 1
                                
    check_success = list(map(len,[cloud_entry,local_entry,cloud_relations,local_relations,mapping]))    # list with length of all snapshot.db table lists
    
    if all(v == check_success[0] for v in check_success):
        success = True
    else:
        print('Error: snapshot.db table lists (from matched files) have different lengths.')
        
    c_c.close
    cloud.close
    
    return success, check_success[0], cloud_entry, local_entry, cloud_relations, local_relations, mapping
    

# DRIVE FILES ------------------------------------------------------------------------------------

# sync Google Drive - temporary function for starting sync - SHOULD BE CHANGED
def sync_google_drive():
    
    # md5_check - boolean for md5 checksums
    # setting md5_check to true is safer but memory inefficient.
    global md5_check, start_from_scratch
    md5_check = False

    home =  os.path.expanduser('~')
    drive_user_path = home+'/Library/Application Support/Google/Drive/user_default'
    document_user_path = home+'/Documents'

    current_config_path = os.path.join(drive_user_path,'sync_config.db')
    current_snapshot_path = os.path.join(drive_user_path,'snapshot.db')
    current_cloud_path = os.path.join(drive_user_path,'cloud_graph','cloud_graph.db')

    drive_path_1 = '/Users/danbonness/Downloads/Test'
    drive_path_2 = '/Users/danbonness/Downloads/Test 2'
    drive_path_3 = '/Users/danbonness/Documents/Test 4'
    drive_path_4 = '/Volumes/Remote LaCie - C/Drive'
    drive_path_5 = '/Volumes/Remote LaCie - C/Social Drive'

    config_path = 'Testing Files/sync_config.db'
    snapshot_path = 'Testing Files/snapshot.db'

    drive_path = drive_path_5

    use_current_files = False  # Should the program edit the files currently residing in the drive_user_path?
    start_from_scratch = True   # Should the program create a snapshot.db from scratch? (False - try to use a pre-existing file?)
    start_gdrive = True # Should the program start Backup & Sync after completion?
    
    sucess = False;
    
    if(start_from_scratch):
        new_snapshot(snapshot_path) # create a new snapshot.db file
    if(use_current_files):
        success = initialize_drive_files(current_config_path,current_snapshot_path, current_cloud_path, drive_path)
        delete_path_files(drive_user_path,['cloud_graph','sync_config.db','snapshot.db'])
    else:
        success = initialize_drive_files(config_path,snapshot_path, current_cloud_path, drive_path)
        copy_drive_files(config_path,snapshot_path, drive_user_path)
        
    if(success and start_gdrive):
        subprocess.call(['/bin/bash','-c','open "/Applications/Backup and Sync.app"'])  # start Backup & Sync process
        print('\nProcess started: Backup & Sync')

# initialize drive setup files
def initialize_drive_files(config_path,snapshot_path, cloud_path,drive_path):
    
    success = False # Did the program successfully initialize the drive files?
    
    close_process('Backup and Sync', True)  # close Backup & Sync process, kill immediately
    
    if(os.path.isfile(config_path)):    # if sync_config.db exists at config_path
        update_sync_config(config_path, drive_path)
        success = update_snapshot(snapshot_path, cloud_path, drive_path)
    else:
        print('Please provide a sync_config.db file')
        
    return success

# copy drive setup files
def copy_drive_files(config_path,snapshot_path, drive_user_path):
    delete_path_files(drive_user_path,['cloud_graph'])
    copy_file (config_path, drive_user_path)
    copy_file (snapshot_path, drive_user_path)
    
# backup drive configuration files for safe keeping
def backup_drive_files(drive_user_path,dest_path):
    
    current_snapshot_path = os.path.join(drive_user_path,'snapshot.db')
    current_config_path = os.path.join(drive_user_path,'sync_config.db')
    current_cloud_path = os.path.join(drive_user_path,'cloud_graph','cloud_graph.db')
    
    backup_folder_name = 'GDrive Backup' # name of main folder where all configuration files will be stored
    
    if(os.path.isfile(current_snapshot_path)):
        
        snapshot = sqlite3.connect(current_snapshot_path)
        c_sn = snapshot.cursor()
        
        snapshot_volume_label = c_sn.execute('SELECT label FROM volume_info') # find root inode of current snapshot.db
        volume_label = snapshot_volume_label.fetchone()[0]
        
        c_sn.close
        snapshot.close
        
        backup_path = os.path.join(dest_path, backup_folder_name, volume_label) # location to backup configuration files
        cloud_path = os.path.join(dest_path, backup_folder_name,'cloud_graph')
    
        if(not os.path.exists(backup_path)):    # if the backup path does not exist
            try:
                os.makedirs(backup_path)    # make it
            except:
                print('Error: Failed to create main folder for Google Drive file backups')
                
        if(os.path.exists(backup_path)):    # if the backup path exists
            copy_file (current_snapshot_path, backup_path)  # copy snapshot.db
            copy_file (current_config_path, backup_path)    # copy sync_config.db
            if(not os.path.exists(cloud_path)):    # if the cloud_graph backup path does not exist
                try:
                    os.makedirs(cloud_path)    # make it
                except:
                    print('Error: Failed to create cloud_graph folder for Google Drive file backups')
            
            if(os.path.exists(cloud_path)):    # if the cloud_graph backup does exist
                copy_file(current_cloud_path,cloud_path)    # copy cloud_graph.db

# GUI ------------------------------------------------------------------------------------

                
# Tkinter GUI             
def start_gui():
    
    home =  os.path.expanduser('~')
    drive_user_path = home+'/Library/Application Support/Google/Drive/user_default'
    document_user_path = home+'/Documents'

    current_config_path = os.path.join(drive_user_path,'sync_config.db')
    current_snapshot_path = os.path.join(drive_user_path,'snapshot.db')
    current_cloud_path = os.path.join(drive_user_path,'cloud_graph','cloud_graph.db')

    drive_path_1 = '/Users/danbonness/Downloads/Test'
    drive_path_2 = '/Users/danbonness/Downloads/Test 2'
    drive_path_3 = '/Users/danbonness/Documents/Test 4'
    drive_path_4 = '/Volumes/Remote LaCie - C/Drive'
    drive_path_5 = '/Volumes/Remote LaCie - C/Social Drive'

    config_path = 'Testing Files/sync_config.db'
    snapshot_path = 'Testing Files/snapshot.db'

    drive_path = drive_path_5
    
    root = Tk()
    root.title("Google Drive Update")
    root.geometry("700x500")
    root.grid_columnconfigure(1, weight=1)
    
    # Tkinter Configuration Variables

    path_display = [StringVar(), StringVar()]
    dir_location = ["",""]
    
    output_text = StringVar()
    progress_text = StringVar()
    generic_checkbox = IntVar()

    '''
    try:
        updatePath(0, csv_text[0][0])
    except:
        pass
    '''

    nb = ttk.Notebook(root)
    page1 = Frame(nb)
    page2 = Frame(nb)

    ttk.Style().configure('TNotebook', tabposition='nw') # 'ne' as in compass direction

    page1.grid_columnconfigure(1, weight=1)
    page2.grid_columnconfigure(1, weight=1)

    nb.add(page1, text = 'Sync Google Drive')
    nb.add(page2, text = 'Settings')

    # Tab 1 - Sync Google Drive
    
    # Constant Labels
    
    Label(page1,text="Sync Google Drive",font=("TkDefaultFont",20)).grid(row=0, columnspan = 2)
    Label(page1,text="Directory:",font=("TkDefaultFont",16)).grid(row=1, sticky=W)

    # Variable Labels
    
    path_label = Label(page1, textvariable=path_display[0],anchor=E)
    path_label.config(relief="solid",borderwidth=1)

    output_label = Label(page1, textvariable=output_text)
    progress_label = Label(page1, textvariable=progress_text)
    
    path_label.grid(row=2,column=1,padx=5,sticky=EW)
    output_label.grid(row=5,columnspan=2)
    progress_label.grid(row=6,columnspan=2)
    
    # Buttons

    path_button = Button(page1, text="Choose Path", command=lambda : choosePath(0))
    merge_button = Button(page1, text="Merge Directories", command=lambda : get_top_level(), font=("TkDefaultFont",16))
    test_button = Button(page1, text="Test", command=lambda : test(), font=("TkDefaultFont",16))
    
    path_button.grid(row=2)
    merge_button.grid(row=3,columnspan=2,pady=10)
    test_button.grid(row=4,columnspan=2,pady=0)
    
    # Tab 2 - Settings
    
    # Constant Labels
    
    Label(page2,text="Settings",font=("TkDefaultFont",20)).grid(row=0, columnspan = 2)
    
    Label(page2,text="Initial Configuration Files:",font=("TkDefaultFont",16)).grid(row=1, sticky=W, columnspan = 2)
    
    Label(page2,text="sync_config.db:",font=("TkDefaultFont",12)).grid(row=2, sticky=W)
    Label(page2,text="snapshot.db:",font=("TkDefaultFont",12)).grid(row=4, sticky=W)
    
    Label(page2,text="Drive Sync Location:",font=("TkDefaultFont",16)).grid(row=6, sticky=W, columnspan = 2,pady=(10,0))
    
    Label(page2,text="User Settings:",font=("TkDefaultFont",16)).grid(row=8, columnspan = 2, sticky=W,pady=(10,0))
    
    Label(page2,text="Additional Settings:",font=("TkDefaultFont",16)).grid(row=12, columnspan = 2, sticky=W,pady=(10,0))
    
    # Variable Labels 
    
    tab_2_label_config_1 = Label(page2, textvariable=path_display[0],anchor=E)
    tab_2_label_config_2 = Label(page2, textvariable=path_display[0],anchor=E)
    tab_2_label_sync_1 = Label(page2, textvariable=path_display[0],anchor=E)
    
    tab_2_label_config_1.config(relief="solid",borderwidth=1)
    tab_2_label_config_2.config(relief="solid",borderwidth=1)
    tab_2_label_sync_1.config(relief="solid",borderwidth=1)
    
    tab_2_label_config_1.grid(row=3,column=1,padx=5,sticky=EW)
    tab_2_label_config_2.grid(row=5,column=1,padx=5,sticky=EW)
    tab_2_label_sync_1.grid(row=7,column=1,padx=5,sticky=EW)
    
    # Checkboxes
    
    Checkbutton(page2, text="Use root configuration files (files currently residing in user_default)", variable=generic_checkbox).grid(row=9, sticky=W, columnspan = 2)
    Checkbutton(page2, text="Generate new snapshot.db file", variable=generic_checkbox).grid(row=10, sticky=W, columnspan = 2)
    Checkbutton(page2, text="Use MD5 checksum to compare files (slow, not recommended)", variable=generic_checkbox).grid(row=11, sticky=W, columnspan = 2)
    
    Checkbutton(page2, text="Start Backup & Sync on exit", variable=generic_checkbox).grid(row=13, sticky=W, columnspan = 2)
    
    # Buttons
    
    tab_2_button_config_1 = Button(page2, text="Choose File", command=lambda : choosePath(0))
    tab_2_button_config_2 = Button(page2, text="Choose File", command=lambda : choosePath(0))
    tab_2_button_sync_1 = Button(page2, text="Choose Path", command=lambda : choosePath(0))
    
    tab_2_button_config_1.grid(row=3)
    tab_2_button_config_2.grid(row=5)
    tab_2_button_sync_1.grid(row=7)
    
    # Add Elements to Notebook & Run Tkinter Program

    nb.pack(expand=1, fill="both")

    root.mainloop()
                
# PROGRAM START ------------------------------------------------------------------------------------

#backup_drive_files(drive_user_path,document_user_path)
#sync_google_drive()

start_gui()
        
'''
start = time.time()
end = time.time()
print(end - start)
'''