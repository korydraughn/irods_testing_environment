#!/usr/bin/env python3
"""
Diagnostic for iRODS GET operation issue
"""

import os
import tempfile
import time
from irods.session import iRODSSession
from irods.exception import iRODSException

def main():
    print("=== iRODS GET Diagnostic ===\n")
    
    env_file = os.path.expanduser('~/.irods/irods_environment.json')
    session = iRODSSession(irods_env_file=env_file)
    
    print(f"Connected as: {session.username}@{session.zone}")
    print(f"Host: {session.host}:{session.port}\n")
    
    home_path = f"/{session.zone}/home/{session.username}"
    test_path = f"{home_path}/.test_file_{int(time.time())}.dat"
    
    # Create a small test file locally
    print("1. Creating local test file...")
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as tmp:
        tmp.write(b"Hello iRODS!")
        local_file = tmp.name
    print(f"   ✓ Created: {local_file}")
    
    try:
        # Upload the file
        print(f"\n2. Uploading to iRODS: {test_path}")
        session.data_objects.put(local_file, test_path)
        print(f"   ✓ Upload successful")
        
        # Check if file exists
        print(f"\n3. Verifying file exists in iRODS...")
        try:
            obj = session.data_objects.get(test_path)
            print(f"   ✓ File exists")
            print(f"   Size: {obj.size} bytes")
            print(f"   Path: {obj.path}")
            print(f"   Name: {obj.name}")
            print(f"   Owner: {obj.owner_name}")
        except Exception as e:
            print(f"   ✗ Cannot verify file")
            print(f"   Error: {e}")
        
        # Try different download methods
        print(f"\n4. Testing download methods...")
        
        # Method 1: Direct get to file
        print(f"\n   Method 1: data_objects.get() to file")
        try:
            download_file = tempfile.mktemp(suffix='.dat')
            session.data_objects.get(test_path, download_file)
            print(f"   ✓ Method 1 SUCCESS")
            if os.path.exists(download_file):
                print(f"   Downloaded size: {os.path.getsize(download_file)} bytes")
                os.unlink(download_file)
        except iRODSException as e:
            print(f"   ✗ Method 1 FAILED")
            print(f"   Error: {e}")
            print(f"   Code: {e.code if hasattr(e, 'code') else 'N/A'}")
            print(f"   Message: {e.msg if hasattr(e, 'msg') else 'N/A'}")
        
        # Method 2: Open and read
        print(f"\n   Method 2: open() and read()")
        try:
            with session.data_objects.open(test_path, 'r') as f:
                content = f.read()
            print(f"   ✓ Method 2 SUCCESS")
            print(f"   Content: {content}")
        except iRODSException as e:
            print(f"   ✗ Method 2 FAILED")
            print(f"   Error: {e}")
            print(f"   Code: {e.code if hasattr(e, 'code') else 'N/A'}")
        
        # Method 3: Get object and open
        print(f"\n   Method 3: Get object then open()")
        try:
            obj = session.data_objects.get(test_path)
            with obj.open('r') as f:
                content = f.read()
            print(f"   ✓ Method 3 SUCCESS")
            print(f"   Content: {content}")
        except iRODSException as e:
            print(f"   ✗ Method 3 FAILED")
            print(f"   Error: {e}")
            print(f"   Code: {e.code if hasattr(e, 'code') else 'N/A'}")
        
        # Check permissions
        print(f"\n5. Checking file permissions...")
        try:
            obj = session.data_objects.get(test_path)
            print(f"   Owner: {obj.owner_name}")
            print(f"   Replicas: {len(obj.replicas)}")
            for i, replica in enumerate(obj.replicas):
                print(f"   Replica {i}:")
                print(f"     Resource: {replica.resource_name}")
                print(f"     Status: {replica.status}")
                print(f"     Path: {replica.path}")
        except Exception as e:
            print(f"   ✗ Cannot check permissions: {e}")
        
        # Check user permissions
        print(f"\n6. Checking user info...")
        try:
            users = session.users.get(session.username, session.zone)
            print(f"   User type: {users[0].type}")
            print(f"   User zone: {users[0].zone}")
        except Exception as e:
            print(f"   Cannot get user info: {e}")
        
        # Cleanup
        print(f"\n7. Cleaning up...")
        session.data_objects.unlink(test_path, force=True)
        print(f"   ✓ Cleanup successful")
        
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        if hasattr(e, 'code'):
            print(f"   Code: {e.code}")
    finally:
        if os.path.exists(local_file):
            os.unlink(local_file)
        session.cleanup()
    
    print(f"\n=== Diagnostic Complete ===")

if __name__ == "__main__":
    main()
