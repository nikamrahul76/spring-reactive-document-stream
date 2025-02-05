package com.rn.study.document.steaming.controller;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

@RestController
public class DocumentServiceController {

    @GetMapping(value = "/stream", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<Flux<DataBuffer>> streamPdf() {
        try {
            Path path = new ClassPathResource("Prajakta.pdf").getFile().toPath();
            DefaultDataBufferFactory bufferFactory = DefaultDataBufferFactory.sharedInstance;
            Flux<DataBuffer> dataBufferFlux = Flux.using(
                    () -> Files.newInputStream(path), // Open the InputStream for the file
                    inputStream -> Flux.generate(sink -> { // Explicitly specify DataBuffer type
                        try {
                            byte[] buffer = new byte[8192]; // 8 KB buffer size
                            int bytesRead = inputStream.read(buffer);
                            if (bytesRead == -1) {
                                sink.complete(); // End of file
                            } else {
                                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead); // Wrap exact bytes
                                DataBuffer dataBuffer = bufferFactory.wrap(byteBuffer); // Create DataBuffer
                                sink.next(dataBuffer); // Emit DataBuffer
                            }
                        } catch (IOException e) {
                            sink.error(e); // Emit the error
                        }
                    }),
                    inputStream -> { // Close the InputStream
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            throw new RuntimeException("Error closing InputStream: " + e.getMessage(), e);
                        }
                    }
            );

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.setContentDispositionFormData("attachment", "Prajakta.pdf"); // Downloadable filename

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(dataBufferFlux);

        } catch (IOException e) {
            return ResponseEntity.internalServerError()
                    .body(Flux.error(new RuntimeException("Error streaming file: " + e.getMessage())));
        }
    }
}