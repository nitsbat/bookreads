package com.example;

import com.example.author.Author;
import com.example.author.AuthorRepository;
import com.example.books.Book;
import com.example.books.BookRepository;
import com.example.connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.example.ApplicationConstants.NO_AUTHOR_NAME;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class DemoApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;


    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    @Value("${datadump.location.authors" +
            "}")
    private String authorsDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle
     * to connect to the database
     */
    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

    public void initAuthors() {

        Path path = Path.of(authorsDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(
                    line -> {
                        String substring = line.substring(line.indexOf("{"));
                        try {
                            JSONObject jsonObject = new JSONObject(substring);

                            Author author = new Author();
                            author.setName(jsonObject.optString("name"));
                            author.setId(jsonObject.optString("key").replace("/authors/", ""));

                            System.out.println("Author saved : " + author.getName());
                            authorRepository.save(author);

                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
            );
        } catch (IOException e) {
        }
    }

    @PostConstruct
    public void start() {
        initAuthors();
        initWorks();
    }

    private void initWorks() {
        Path path = Path.of(worksDumpLocation);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        List<String> workLines = null;
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(
                    line -> {
                        String substring = line.substring(line.indexOf("{"));
                        try {
                            JSONObject jsonObject = new JSONObject(substring);

                            Book book = new Book();
                            book.setId(jsonObject.optString("key").replace("/works/", ""));
                            book.setName(jsonObject.optString("title"));

                            List<String> coverIds = new ArrayList<>();
                            JSONArray covers = jsonObject.optJSONArray("covers");
                            if (covers != null) {
                                for (int i = 0; i < covers.length(); i++) {
                                    coverIds.add(covers.getString(i));
                                }
                                book.setCoverIds(coverIds);
                            }

                            List<String> authorIds = new ArrayList<>();
                            JSONArray authorIdArr = jsonObject.optJSONArray("authors");
                            if (authorIdArr != null) {
                                for (int i = 0; i < authorIdArr.length(); i++) {
                                    String id = authorIdArr.getJSONObject(i).optJSONObject("author").optString("key").replace("/authors/", "");
                                    authorIds.add(id);
                                }
                                book.setAuthorIds(authorIds);
                            }


                            List<String> authorNames = authorIds.stream()
                                    .map(authorId -> authorRepository.findById(authorId))
                                    .map(author -> {
                                                if (!author.isPresent()){
                                                    return NO_AUTHOR_NAME;
                                                }
                                                else
                                                    return author.get().getName();
                                            }
                                    ).collect(Collectors.toList());
                            book.setAuthorNames(authorNames);

                            JSONObject createdDate = jsonObject.optJSONObject("created");
                            if (createdDate != null) {
                                String date = createdDate.optString("value");
                                book.setCreatedBy(LocalDate.parse(date, dateTimeFormatter));
                            }

                            System.out.println("Book saved : " + book.getName());
                            bookRepository.save(book);

                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
            );
        } catch (Exception ex) {

        }
    }
}
