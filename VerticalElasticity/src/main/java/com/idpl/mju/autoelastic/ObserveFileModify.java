package com.idpl.mju.autoelastic;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.LocalDateTime;

public class ObserveFileModify {
	public static void observeFileModified(String targetDirectory) {

       Path faxFolder = Paths.get(targetDirectory);

       try {
           WatchService fileWatchService = FileSystems.getDefault().newWatchService();
           faxFolder.register(fileWatchService, StandardWatchEventKinds.ENTRY_MODIFY);
           boolean valid = true;
           do {
               WatchKey watchKey = fileWatchService.take();
               for (WatchEvent event : watchKey.pollEvents()) {
                   WatchEvent.Kind kind = event.kind();
                   if (StandardWatchEventKinds.ENTRY_MODIFY.equals(event.kind())) {
                       String fileName = event.context().toString();
//                       System.out.println("start to notify file Modified :" + fileName + " , time : " + LocalDateTime.now());
                   }
               }
               valid = watchKey.reset();
           } while (valid);
       } catch (IOException | InterruptedException e) {
           e.printStackTrace();
       }
   }
}
