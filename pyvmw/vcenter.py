# Class for holding variables related to a site.
from pyVim.connect import SmartConnect, Disconnect
from pyVim.task import WaitForTask
from pyVmomi import vim, vmodl
import ssl
import datetime
import logging
import socket
from logging.handlers import RotatingFileHandler

class vcsite:
    def __init__(self, host, username, password, port=443, verify_ssl=False, loglevel="INFO", logger=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.version = None
        self.__conn__ = None
        self.LOGLEVEL = loglevel.upper()
        self.log = None

        if logger is None:
            #set log line format including container_id
            container_id = str(socket.gethostname())
            log_formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(threadName)s;%(message)s", "%Y-%m-%d %H:%M:%S")
            log_handler = RotatingFileHandler(filename=f"./logs/Log-{container_id}.log", maxBytes=1024*1024*100, backupCount=5)
            log_handler.setFormatter(log_formatter)
            self.log = logging.getLogger("vCenter Module")
            self.log.setLevel(self.LOGLEVEL)
            self.log.addHandler(log_handler)
        else:
            self.log = logger

    def connect(self):
        """
        Establish a connection to the vCenter server.

        This method creates a connection to the vCenter server using the provided
        credentials and SSL context.

        Logs the connection status and retrieves the vCenter version upon success.
        """
        self.log.info(f"Log Level set to {self.LOGLEVEL}")
        if self.__conn__ is None:
            context = ssl.create_default_context()
            if not self.verify_ssl:
                self.log.debug("dont verify SSL")
                # Create an SSL context without certificate verification
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE

            # connect to vCenter Server
            si = None
            try:
                self.__conn__ = SmartConnect(host=self.host, user=self.username, pwd=self.password, sslContext=context)
                about_info = self.__conn__.content.about
                version = about_info.version
                self.version = version
                self.log.debug("Connected to vCenter Server %s", self.host)
            except Exception as e:
                self.log.error(f"Error connecting to vCenter Server: {e}")

    def version(self):
        """
        Retrieve the version of the connected vCenter server.

        Returns:
            str: The version of the vCenter server, or None if not connected.
        """
        return self.version

    def datastore_list(self):
        """
        Retrieve a list of all datastores in the vCenter.

        Returns:
            list: A list of datastore names, or an error message if something goes wrong.
        """
        if self.__conn__ is None:
            self.log.debug("Trying to retrieve datastore list without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            datastores = []

            # Get all datastores
            for ds in content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True).view:
                datastores.append(ds.name)

            self.log.info(f"Retrieved {len(datastores)} datastores.")
            return datastores

        except Exception as e:
            self.log.error(f"Error retrieving datastore list: {e}")
            return {"Error": str(e)}

    def find_iso(self, datastore_name, iso_name):
        """
        Search a datastore for an ISO file and return its path.

        Args:
            datastore_name (str): Name of the datastore to search.
            iso_name (str): Name of the ISO file to find.

        Returns:
            str: Full path to the ISO file in the format 'datastore_name/folder/filename.iso',
                or an error message if not found.
        """
        if not datastore_name or not iso_name:
            self.log.error("Both datastore name and ISO file name are required.")
            return {"Error": "Datastore name and ISO file name are required."}

        if self.__conn__ is None:
            self.log.debug("Trying to search datastore without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()

            # Find the datastore
            datastore = None
            for ds in content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True).view:
                if ds.name == datastore_name:
                    datastore = ds
                    break

            if not datastore:
                self.log.error(f"Datastore '{datastore_name}' not found.")
                return {"Error": f"Datastore '{datastore_name}' not found."}

            # Browse the datastore for the ISO
            browser = datastore.browser
            task = browser.SearchDatastore_Task(
                datastorePath=f"[{datastore_name}]",
                searchSpec=vim.HostDatastoreBrowser.SearchSpec(
                    matchPattern=[iso_name],
                    details=vim.FileQueryFlags(fileSize=True, fileType=True, modification=True)
                )
            )
            self.log.info(f"Searching for ISO '{iso_name}' in datastore '{datastore_name}'.")
            WaitForTask(task)
            search_results = task.info.result

            if search_results and search_results.file:
                for file in search_results.file:
                    # Match the file name to the iso_name
                    if file.path.endswith(iso_name):
                        full_path = f"{datastore_name}/{file.path}"
                        self.log.info(f"ISO '{iso_name}' found in datastore '{datastore_name}': {full_path}")
                        return full_path

            self.log.warning(f"ISO '{iso_name}' not found in datastore '{datastore_name}'.")
            return {"Error": f"ISO '{iso_name}' not found in datastore '{datastore_name}'."}

        except Exception as e:
            self.log.error(f"Error searching for ISO '{iso_name}' in datastore '{datastore_name}': {e}")
            return {"Error": str(e)}

    def find_iso_in_all_datastores(self, iso_name):
        """
        Search all datastores for a given ISO file and return its path as soon as it is found.

        Args:
            iso_name (str): Name of the ISO file to find.

        Returns:
            str: Full path to the ISO file in the format 'datastore_name/folder/filename.iso',
                or an error message if the ISO is not found in any datastore.
        """
        if not iso_name:
            self.log.error("ISO file name is required.")
            return {"Error": "ISO file name is required."}

        # Get the list of datastores
        datastores = self.datastore_list()

        if isinstance(datastores, dict) and "Error" in datastores:
            return datastores  # Return error if datastore_list failed

        for datastore_name in datastores:
            self.log.info(f"Searching for ISO '{iso_name}' in datastore '{datastore_name}'...")
            result = self.find_iso(datastore_name=datastore_name, iso_name=iso_name)

            if isinstance(result, str):  # If an ISO path is returned
                self.log.info(f"ISO '{iso_name}' found in datastore '{datastore_name}': {result}")
                return result

            # If result is an error, continue searching other datastores
            self.log.warning(f"ISO '{iso_name}' not found in datastore '{datastore_name}'.")

        self.log.error(f"ISO '{iso_name}' not found in any datastore.")
        return {"Error": f"ISO '{iso_name}' not found in any datastore."}

    def get_cpu_mem_used(self, vra=None):
        """
        Retrieve the CPU and memory usage of a specified VM.

        Args:
            vra (str): Name of the VM to retrieve the stats for.

        Returns:
            list: A list containing the CPU usage (MHz) and memory usage (MB) of the VM,
                  or None if the VM is not found.
        
        Raises:
            ValueError: If the VM is not found in the vCenter.
        """
        if vra == None:
            self.log.debug("Get_cpu_mem_used called with no vm name...returning no data")
            return
        if self.__conn__ == None:
            self.log.debug("Trying to get VRA stats without vCenter connection, trying to connect")
            self.connect()

        # get the root folder of the vCenter Server
        try:
            content = self.__conn__.RetrieveContent()
            root_folder = content.rootFolder
        except:
            self.log.debug("Could not get content from vCenter when trying to get VRA stats")

        # create a view for all VMs on the vCenter Server
        view_manager = content.viewManager
        vm_view = view_manager.CreateContainerView(root_folder, [vim.VirtualMachine], True)

        vm = None
        for vm_obj in vm_view.view:
            if str(vm_obj.name) == str(vra):
                vm = vm_obj
            if vm is not None:
                self.log.debug(f"Found VRA VM in vCenter with name {vm.name}")
                # get the CPU usage and memory usage for the VM
                cpu_usage_mhz = vm.summary.quickStats.overallCpuUsage
                memory_usage_mb = vm.summary.quickStats.guestMemoryUsage

                # print the CPU and memory usage for the VM
                self.log.info(f"VM {vm.name} has CPU usage of {cpu_usage_mhz} MHz and memory usage of {memory_usage_mb} MB")
                return [cpu_usage_mhz, memory_usage_mb]
            else:
                self.log.debug(f"{vm_obj.name} is not a VRA")
        raise ValueError("No VRA Found")

    def get_vm_list(self):
        """
        Retrieve a list of all VMs in the vCenter Server.
        Returns:
            list: A list of VM names.
        """
        if self.__conn__ is None:
            self.log.debug("Trying to get VM list without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            root_folder = content.rootFolder
            view_manager = content.viewManager
            vm_view = view_manager.CreateContainerView(root_folder, [vim.VirtualMachine], True)

            vm_list = [vm_obj.name for vm_obj in vm_view.view]
            vm_view.Destroy()
            self.log.info(f"Retrieved {len(vm_list)} virtual machines from vCenter.")
            return vm_list
        except Exception as e:
            self.log.error(f"Error while retrieving VM list: {e}")
            return []

    def list_vm_datastores(self, vm):
        """
        List all datastores accessible by the specified VM.

        Args:
            vm (str): Name of the VM.

        Returns:
            list: A list of datastore names accessible by the VM, or an error message.
        """
        if not vm:
            return {"Error": "VM name is required."}

        if self.__conn__ is None:
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                return {"Error": f"VM '{vm}' not found."}

            datastores = [ds.name for ds in vm_obj.datastore]
            return datastores
        except Exception as e:
            return {"Error": str(e)}

    def vm_add_cdrom_drive(self, vm):
        """
        Add a CD-ROM drive to the specified VM with a client device backing.

        Args:
            vm (str): Name of the VM to add the CD-ROM drive to.

        Returns:
            dict: A dictionary with the VM name as the key and a success message or error.
        """
        if not vm:
            self.log.error("VM name is required to add a CD-ROM drive.")
            return {"Error": "VM name is required."}

        if self.__conn__ is None:
            self.log.debug("Trying to add CD-ROM drive without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                self.log.error(f"VM '{vm}' not found in vCenter.")
                return {vm: "Not Found"}

            # Create a specification for adding a CD-ROM drive
            spec = vim.vm.ConfigSpec()
            device_changes = []

            # Define a new CD-ROM drive
            cdrom = vim.vm.device.VirtualCdrom()
            cdrom.key = -1  # Unique key, negative value signifies it's a new device
            cdrom.controllerKey = 200  # Default IDE controller (usually 200 or 201 for most VMs)
            cdrom.unitNumber = 0  # First device on the controller
            cdrom.backing = vim.vm.device.VirtualCdrom.RemoteAtapiBackingInfo()  # Client device backing
            cdrom.connectable = vim.vm.device.VirtualDevice.ConnectInfo(
                startConnected=False,  # The CD-ROM drive starts disconnected
                allowGuestControl=True,
                connected=False
            )

            # Create a device change spec to add the CD-ROM
            device_spec = vim.vm.device.VirtualDeviceSpec()
            device_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
            device_spec.device = cdrom
            device_changes.append(device_spec)

            # Add the device changes to the VM config spec
            spec.deviceChange = device_changes

            # Reconfigure the VM to add the CD-ROM drive
            task = vm_obj.ReconfigVM_Task(spec=spec)
            self.log.info(f"Adding CD-ROM drive to VM '{vm}' with client device backing.")
            WaitForTask(task)
            self.log.info(f"Successfully added CD-ROM drive to VM '{vm}'.")
            return {vm: "CD-ROM drive added successfully"}

        except Exception as e:
            self.log.error(f"Error adding CD-ROM drive to VM '{vm}': {e}")
            return {"Error": str(e)}

    def vm_cdrom_load_iso(self, vm, iso):
        """
        Load an ISO file into the CD-ROM drive of a VM.

        Args:
            vm (str): Name of the VM.
            iso (str): Path to the ISO file in the format 'datastore_name/folder/filename.iso'.

        Returns:
            dict: A dictionary with the VM name as the key and a success message or error.
        """
        if not vm or not iso:
            self.log.error("Both VM name and ISO file path are required to load an ISO.")
            return {"Error": "VM name and ISO file path are required."}

        if self.__conn__ is None:
            self.log.debug("Trying to load ISO without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                self.log.error(f"VM '{vm}' not found in vCenter.")
                return {vm: "Not Found"}

            # Split the ISO path into datastore name and file path
            if '/' not in iso:
                self.log.error("ISO path format is invalid. Expected 'datastore_name/folder/filename.iso'.")
                return {"Error": "Invalid ISO path format"}
            datastore_name, file_path = iso.split('/', 1)

            # Find the datastore in vCenter
            datastore = None
            for ds in content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True).view:
                if ds.name == datastore_name:
                    datastore = ds
                    break

            if not datastore:
                self.log.error(f"Datastore '{datastore_name}' not found in vCenter.")
                return {"Error": f"Datastore '{datastore_name}' not found in vCenter."}

            # Check for an existing CD-ROM drive
            cdrom_device = None
            for device in vm_obj.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualCdrom):
                    cdrom_device = device
                    break

            if not cdrom_device:
                self.log.error(f"VM '{vm}' does not have a CD-ROM drive.")
                return {vm: "No CD-ROM drive found"}

            # Configure the CD-ROM to use the ISO file
            cdrom_device.backing = vim.vm.device.VirtualCdrom.IsoBackingInfo()
            cdrom_device.backing.fileName = f"[{datastore_name}] {file_path}"
            cdrom_device.connectable = vim.vm.device.VirtualDevice.ConnectInfo(
                startConnected=True,
                allowGuestControl=True,
                connected=True
            )

            # Create a device change spec
            device_spec = vim.vm.device.VirtualDeviceSpec()
            device_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
            device_spec.device = cdrom_device

            # Apply the device change
            spec = vim.vm.ConfigSpec()
            spec.deviceChange = [device_spec]

            task = vm_obj.ReconfigVM_Task(spec=spec)
            self.log.info(f"Loading ISO '{iso}' into CD-ROM drive of VM '{vm}'.")
            WaitForTask(task)
            self.log.info(f"Successfully loaded ISO '{iso}' into CD-ROM drive of VM '{vm}'.")
            return {vm: f"ISO '{iso}' loaded successfully"}

        except Exception as e:
            self.log.error(f"Error loading ISO '{iso}' into CD-ROM drive of VM '{vm}': {e}")
            return {"Error": str(e)}

    def vm_has_cdrom_drive(self, vm=None):
        """
        Check if the specified VM has a CD-ROM drive in its VMX configuration.
        
        Args:
            vm (str): Name of the VM to check.
        
        Returns:
            dict: A dictionary with the VM name as the key and a boolean indicating if it has a CD-ROM drive.
        """
        if not vm:
            self.log.error("VM name is required to check for a CD-ROM drive.")
            return {"Error": "VM name is required."}

        if self.__conn__ is None:
            self.log.debug("Trying to check for CD-ROM drive without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                self.log.error(f"VM '{vm}' not found in vCenter.")
                return {vm: "Not Found"}

            # Check for a CD-ROM device
            for device in vm_obj.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualCdrom):
                    self.log.info(f"VM '{vm}' has a CD-ROM drive configured.")
                    return {vm: True}

            self.log.info(f"VM '{vm}' does not have a CD-ROM drive configured.")
            return {vm: False}

        except Exception as e:
            self.log.error(f"Error checking for CD-ROM drive on VM '{vm}': {e}")
            return {"Error": str(e)}

    def vm_get_mac_address(self, vm):
        """
        Retrieve the MAC address of a VM. If there are multiple NICs, return a dictionary
        with the adapter name and the MAC address for each NIC.

        Args:
            vm (str): Name of the VM.

        Returns:
            dict: A dictionary where keys are adapter names and values are MAC addresses,
                or an error message if the VM is not found or has no NICs.
        """
        if not vm:
            self.log.error("VM name is required to retrieve MAC address.")
            return {"Error": "VM name is required."}

        if self.__conn__ is None:
            self.log.debug("Trying to retrieve MAC address without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                self.log.error(f"VM '{vm}' not found in vCenter.")
                return {"Error": f"VM '{vm}' not found."}

            mac_addresses = {}
            for device in vm_obj.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualEthernetCard):
                    adapter_name = device.deviceInfo.label  # e.g., "Network adapter 1"
                    mac_address = device.macAddress
                    mac_addresses[adapter_name] = mac_address

            if not mac_addresses:
                self.log.warning(f"VM '{vm}' has no network adapters.")
                return {"Error": "No network adapters found on the VM."}

            return mac_addresses

        except Exception as e:
            self.log.error(f"Error retrieving MAC address for VM '{vm}': {e}")
            return {"Error": str(e)}

    def vm_poweroff(self, vm=None):
        """
        Power off the specified VM.
        
        Args:
            vm (str): Name of the VM to power off.
        
        Returns:
            dict: The updated power state of the VM as a dictionary.
        """
        if not vm:
            self.log.error("VM name is required to power off a VM.")
            return {"Error": "VM name is required."}

        if self.__conn__ is None:
            self.log.debug("Trying to power off VM without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                self.log.error(f"VM '{vm}' not found in vCenter.")
                return {vm: "Not Found"}

            if vm_obj.runtime.powerState == "poweredOff":
                self.log.info(f"VM '{vm}' is already powered off.")
                return {vm: "poweredOff"}

            task = vm_obj.PowerOffVM_Task()
            self.log.info(f"Initiating power-off for VM '{vm}'.")
            WaitForTask(task)
            self.log.info(f"VM '{vm}' has been powered off.")
            return self.get_vm_power_state(vm=vm)

        except Exception as e:
            self.log.error(f"Error powering off VM '{vm}': {e}")
            return {"Error": str(e)}

    def vm_poweron(self, vm=None):
        """
        Power on the specified VM.
        
        Args:
            vm (str): Name of the VM to power on.
        
        Returns:
            dict: The updated power state of the VM as a dictionary.
        """
        if not vm:
            self.log.error("VM name is required to power on a VM.")
            return {"Error": "VM name is required."}

        if self.__conn__ is None:
            self.log.debug("Trying to power on VM without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                self.log.error(f"VM '{vm}' not found in vCenter.")
                return {vm: "Not Found"}

            if vm_obj.runtime.powerState == "poweredOn":
                self.log.info(f"VM '{vm}' is already powered on.")
                return {vm: "poweredOn"}

            task = vm_obj.PowerOnVM_Task()
            self.log.info(f"Initiating power-on for VM '{vm}'.")
            WaitForTask(task)
            self.log.info(f"VM '{vm}' has been powered on.")
            return self.get_vm_power_state(vm=vm)

        except Exception as e:
            self.log.error(f"Error powering on VM '{vm}': {e}")
            return {"Error": str(e)}

    def vm_set_bios_boot_cdrom(self, vm):
        """
        Set the VM to use BIOS boot mode and make the CD-ROM drive the first boot device.

        Args:
            vm (str): Name of the VM.

        Returns:
            dict: A dictionary with the VM name as the key and a success message or error.
        """
        if not vm:
            self.log.error("VM name is required to set BIOS boot order.")
            return {"Error": "VM name is required."}

        if self.__conn__ is None:
            self.log.debug("Trying to set BIOS boot order without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            vm_obj = None
            for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
                if obj.name == vm:
                    vm_obj = obj
                    break

            if not vm_obj:
                self.log.error(f"VM '{vm}' not found in vCenter.")
                return {"Error": f"VM '{vm}' not found."}

            # Check for an existing CD-ROM drive
            cdrom_device = None
            for device in vm_obj.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualCdrom):
                    cdrom_device = device
                    break

            if not cdrom_device:
                self.log.error(f"VM '{vm}' does not have a CD-ROM drive.")
                return {vm: "No CD-ROM drive found"}

            # Set boot options
            boot_options = vim.vm.BootOptions()
            boot_options.efiSecureBootEnabled = False  # Ensure BIOS boot mode (disable EFI secure boot)

            # Configure CD-ROM as the first boot device
            bootable_cdrom = vim.vm.BootOptions.BootableCdromDevice()
            boot_options.bootOrder = [bootable_cdrom]

            # Create a VM config spec
            spec = vim.vm.ConfigSpec()
            spec.bootOptions = boot_options

            # Reconfigure the VM
            task = vm_obj.ReconfigVM_Task(spec=spec)
            self.log.info(f"Setting BIOS boot mode and CD-ROM as the first boot device for VM '{vm}'.")
            WaitForTask(task)
            self.log.info(f"Successfully set BIOS boot mode and CD-ROM as the first boot device for VM '{vm}'.")
            return {vm: "BIOS boot mode set with CD-ROM as the first boot device"}

        except Exception as e:
            self.log.error(f"Error setting BIOS boot order for VM '{vm}': {e}")
            return {"Error": str(e)}
        
    def get_vm_power_state(self, vm=None):
        """
        Retrieve the power state of a specific VM or all VMs in the vCenter.
        
        Args:
            vm (str): Name of the VM to get the power state for. If None, return all VMs and their power states.
        
        Returns:
            dict: A dictionary with VM names as keys and their power states as values.
        """
        if self.__conn__ is None:
            self.log.debug("Trying to get VM power state without vCenter connection, attempting to connect.")
            self.connect()

        try:
            content = self.__conn__.RetrieveContent()
            root_folder = content.rootFolder
            view_manager = content.viewManager
            vm_view = view_manager.CreateContainerView(root_folder, [vim.VirtualMachine], True)

            vm_power_states = {}

            for vm_obj in vm_view.view:
                vm_power_states[vm_obj.name] = str(vm_obj.runtime.powerState)

            vm_view.Destroy()

            if vm:
                # Return power state for the specified VM if it exists
                if vm in vm_power_states:
                    self.log.info(f"Power state of VM '{vm}': {vm_power_states[vm]}")
                    return {vm: vm_power_states[vm]}
                else:
                    self.log.warning(f"VM '{vm}' not found in vCenter.")
                    return {vm: "Not Found"}
            else:
                # Return all VM power states
                self.log.info(f"Retrieved power states for {len(vm_power_states)} VMs.")
                return vm_power_states

        except Exception as e:
            self.log.error(f"Error while retrieving VM power state: {e}")
            return {}

    def get_write_iops(self, vm=None):
        """
        Retrieve the average write IOPS for a VM over the last 60 seconds.

        Args:
            vm (str): Name of the VM to retrieve the write IOPS for.

        Returns:
            float: The average write IOPS for the VM, or None if no data is found.
        """
        try:
            content = self.__conn__.RetrieveContent()
        except:
            self.log.debug("Could not get content from vCenter when trying to get VRA stats")

        # Find the virtual machine by name
        vm_name = str(vm)
        vm = None

        for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
            if obj.name == vm_name:
                vm = obj
                break

        if vm is None:
            print(f"Virtual machine '{vm_name}' not found")
            return

        # Get performance manager
        perf_manager = content.perfManager

        # Define the metric ID for write IOPS (counterId = 6)
        metric_id = vim.PerformanceManager.MetricId(counterId=6, instance="")

        # calculate the last 60 seconds
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=60)

        # Create a query specification for roll-up data
        query_spec = vim.PerformanceManager.QuerySpec(
            entity=vm,
            metricId=[metric_id],
            format="normal",
            startTime=start_time,
            endTime=end_time,
            intervalId=20,  # Use an appropriate interval for the roll-up data
        )


        # Query the performance statistics
        result = perf_manager.QueryStats(querySpec=[query_spec])

        if result:
            # Get the average write IOPS for the last 60 seconds
            average_write_iops = sum(result[0].value[0].value) / len(result[0].value[0].value)
            print(f"Average write IOPS for the last 60 seconds for {vm_name}: {average_write_iops}")
            return average_write_iops
        else:
            return None

    def get_average_write_latency(self, vm=None):
        """
        Retrieve the average write latency for a VM over the last 60 seconds.

        Args:
            vm (str): Name of the VM to retrieve the write latency for.

        Returns:
            float: The average write latency for the VM in milliseconds, or None if no data is found.
        """
        try:
            content = self.__conn__.RetrieveContent()
        except:
            self.log.debug("Could not get content from vCenter when trying to get VM stats")

        # Find the virtual machine by name
        vm_name = str(vm)
        vm = None

        for obj in content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True).view:
            if obj.name == vm_name:
                vm = obj
                break

        if vm is None:
            self.log.debug(f"Virtual machine '{vm_name}' not found")
            return None

        # Get performance manager
        perf_manager = content.perfManager

        # Define the metric ID for write latency (counterId = X) - replace X with the correct counter ID
        # You'll need to find the specific counter ID for write latency in your vSphere environment.
        # The counter for write latency may vary based on your configuration.

        metric_id = vim.PerformanceManager.MetricId(counterId=10, instance="")  # Replace X with the correct counter ID

        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=60)

        # Create a query specification for roll-up data
        query_spec = vim.PerformanceManager.QuerySpec(
            entity=vm,
            metricId=[metric_id],
            format="normal",
            startTime=start_time,
            endTime=end_time,
            intervalId=20,  # Use an appropriate interval for the roll-up data
        )

        # Query the performance statistics
        result = perf_manager.QueryStats(querySpec=[query_spec])

        if result:
            # Get the average write latency for the last 60 seconds
            if result[0].value[0].value:
                average_write_latency = sum(result[0].value[0].value) / len(result[0].value[0].value)
                self.log.info(f"Average write latency for the last 60 seconds for {vm_name}: {average_write_latency}")
                return average_write_latency

        return None

    def disconnect(self):
        """
        Disconnect from the vCenter server.

        This method closes the connection to the vCenter server and resets the internal state.
        """
        if self.__conn__ == None:
            self.log.debug(f"vCenter disconnect requested, but not currently connected.")
            return
        # Disconnect from vCenter
        Disconnect(self.__conn__)
        self.__conn__ = None
        self.version = None
        self.log.debug(f"Disconnected from vCenter")
