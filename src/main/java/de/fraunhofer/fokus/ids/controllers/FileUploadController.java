package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.entities.Distribution;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.persistence.managers.JobManager;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
/**
 * @author Hoang Luan Ta, hoang.luan.ta@fokus.fraunhofer.de
 */

public class FileUploadController {
    private Logger LOGGER = LoggerFactory.getLogger(FileUploadController.class.getName());
    private DataAssetController dataAssetController;

    public FileUploadController(Vertx vertx) {
        dataAssetController = new DataAssetController(vertx);
    }

    private void addToZipFile(List<String> myList, ZipOutputStream zipOut) throws IOException {
        for (String filePath : myList){
            File input = new File(filePath.trim());
            FileInputStream fis = new FileInputStream(input);
            ZipEntry ze = new ZipEntry(input.getName());
            zipOut.putNextEntry(ze);
            byte[] tmp = new byte[4*1024];
            int size;
            while((size = fis.read(tmp)) != -1){
                zipOut.write(tmp, 0, size);
            }
            zipOut.flush();
            fis.close();
        }
        zipOut.close();

    }

    public void uploadFile (RoutingContext routingContext,Handler<AsyncResult<JsonObject>> resultHandler){
        DataAssetDescription dataAssetDescription = new DataAssetDescription();
        List<String> list= new ArrayList<>();
        String tempName;
        String format;
        if(routingContext.fileUploads().size() > 1) {
            format = "ZIP";
            for (FileUpload fileUpload : routingContext.fileUploads()) {
                File fileSource = new File(fileUpload.uploadedFileName());
                File fileDes = new File(fileUpload.fileName());
                try {
                    FileUtils.moveFile(fileSource, fileDes);
                } catch (IOException e) {
                    LOGGER.error(e);
                }
                list.add(fileUpload.fileName().trim());
            }
            tempName = UUID.randomUUID().toString()+".zip";
            FileOutputStream fos ;
            ZipOutputStream zipOut ;
            try {
                fos = new FileOutputStream(tempName);
                zipOut = new ZipOutputStream(new BufferedOutputStream(fos));
                addToZipFile(list,zipOut);
            } catch (IOException e) {
                LOGGER.error(e);
            }
        } else {
            FileUpload fileUpload = routingContext.fileUploads().iterator().next();
            File fileSource = new File(fileUpload.uploadedFileName());
            tempName = new Date().toInstant()+"_"+fileUpload.fileName();
            File fileDes = new File(tempName);
            format = FilenameUtils.getExtension(fileDes.toString()).toUpperCase();
            try {
                FileUtils.moveFile(fileSource, fileDes);
            } catch (IOException e) {
                LOGGER.error(e);
            }

        }
        JsonObject data = new JsonObject(routingContext.request().getFormAttribute("data"));
        Map<String, String> map = new HashMap<String , String>()
        {
            {
                put("datasettitle", data.getString("datasettitle"));
                put("datasetnotes", data.getString("datasetnotes"));
                put("datasetformat", format);
                put("file", tempName);
            }
        };
        dataAssetDescription.setData(map);
        dataAssetDescription.setDatasourcetype("File Upload");
        dataAssetController.add(dataAssetDescription, data.getString("licenseurl"), data.getString("licensetitle"), resultHandler);

    }

    public void getFileUpload( Handler<AsyncResult<File>> resultHandler, Distribution distribution)  {
        String filename = distribution.getFilename();
        File file = new File(filename);
        File parent = new File(System.getProperty("java.io.tmpdir"));
        String baseName = FilenameUtils.getBaseName(filename);
        String extension = FilenameUtils.getExtension(filename);
        File temp = new File(parent, baseName+"."+extension);

        if (temp.exists()) {
            temp.delete();
        }
        try {
            temp.createNewFile();
            Files.copy(file.toPath(),temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            LOGGER.error(ex);
        }
        resultHandler.handle(Future.succeededFuture(temp));
    }
}
