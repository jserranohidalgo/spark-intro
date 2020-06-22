# Frequent Questions

## Q: What docker version should I use if I have Windows 10 Home?
A: Use [Docker Toolbox](https://docs.docker.com/toolbox/toolbox_install_windows/)

## Q: How does is works?
A: You just have to download the latest `.exe`. After the instalation, when you turn it on it runs a Virtual Machine which uses "normal" docker.

## Q: What I must know about this virtual machine?
A: Remember that everything you are doing on your Docker console it's happening on your virtual machine, not in your actual computer!. A common mistake is to launch a local server and try yo reach in *http://localhost:port*. If you want to reach the server you must connect with *http://yourVirtualMachineIp:port*.

## Q: How can I change my virtual machine characteristics (RAM, Cores, Memory,...)?
A: You just have to set it in the Virtual Box configuration. Open the "default" machine (which is created by docker), turn it off if is running and change what you need in its configuration. After that just open docker console again. 

## Known issues:
* On Windows:
  * During sbt [assembly](https://github.com/sbt/sbt-assembly) (Fat-Jar)
    1. On sbt versions pre 1.3.7, assembly may create a "null" directory. [Issue](https://github.com/sbt/sbt/issues/5206)
    2. If during the assembly, you get an `IllegalArgumentException: requirement failed: Source file 'path\META-INF\license' is a directory`, just delete the `target` directory and assembly again. [Issue](https://github.com/sbt/sbt-assembly/issues/390)
