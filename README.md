
voile
=====
simple file backed Map for java.

On the interface it works like a Map,
on the back end it persists all data to a file.


Example:

    File my_file = new File("mydatafile.vl");

    VoileMap<String, String> vm = new VoileMap<String,String>(my_file);

    vm.get(...); // get items that were stored in previous runs

    vm.put(..., ...); // add new items

    vm.remove(...); // and remove

