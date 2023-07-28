/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package net.teilo.tools.find_duplicate_artifacts;

import static java.util.Objects.requireNonNull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.Indexer;
import org.apache.maven.index.context.IndexCreator;
import org.apache.maven.index.context.IndexUtils;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.updater.IndexUpdateRequest;
import org.apache.maven.index.updater.IndexUpdateResult;
import org.apache.maven.index.updater.IndexUpdater;
import org.apache.maven.index.updater.ResourceFetcher;
import org.eclipse.aether.version.InvalidVersionSpecificationException;

/**
 * Collection of some use cases.
 */
@Singleton
@Named
public class FindDuplicates {

    private static final Logger LOG = Logger.getLogger(FindDuplicates.class.getName());

    private final Indexer indexer;

    private final IndexUpdater indexUpdater;

    private final Map<String, IndexCreator> indexCreators;


    @Inject
    public FindDuplicates(Indexer indexer, IndexUpdater indexUpdater, Map<String, IndexCreator> indexCreators) {
        this.indexer = requireNonNull(indexer);
        this.indexUpdater = requireNonNull(indexUpdater);
        this.indexCreators = requireNonNull(indexCreators);
    }

    public void perform(String repo1, String repo2) throws IOException, InvalidVersionSpecificationException {        
        LOG.info("searching " + repo1);
        Set<String> repo1Gavs = obtainGavsFromIndex(repo1);
        LOG.info(repo1 + "contains " + repo1Gavs.size() + " artifacts");
        LOG.info("searching " + repo2);
        Set<String> repo2Gavs = obtainGavsFromIndex(repo2);
        LOG.info(repo2 + "contains " + repo2Gavs.size() + " artifacts");
        
        repo2Gavs.retainAll(repo1Gavs);
        
        if (repo2Gavs.isEmpty()) {
            System.out.println("no duplicates found");
        } else {
            System.out.println("Duplicates Artifacts found:");
            repo2Gavs.forEach(it -> System.out.println(it));
        }
    }
    
    public Set<String> obtainGavsFromIndex(String repo) throws IOException {
        // Files where local cache is (if any) and Lucene Index should be located
        String repoId = repo.replaceAll(".*:", "").replace('/', '_');
        File repoLocalCache = new File(repoId+"-cache");
        File repoIndexDir = new File(repoId+"-index");
        

        // Creators we want to use (search for fields it defines)
        List<IndexCreator> indexers = new ArrayList<>();
        indexers.add(requireNonNull(indexCreators.get("min")));
        indexers.add(requireNonNull(indexCreators.get("jarContent")));
        indexers.add(requireNonNull(indexCreators.get("maven-plugin")));

        // Create context for central repository index
        IndexingContext centralContext = indexer.createIndexingContext(
                repoId + "-context",
                repoId,
                repoLocalCache,
                repoIndexDir,
                repo,
                null,
                true,
                true,
                indexers);

        // Update the index (incremental update will happen if this is not 1st run and files are not deleted)
        // This whole block below should not be executed on every app start, but rather controlled by some configuration
        // since this block will always emit at least one HTTP GET. Central indexes are updated once a week, but
        // other index sources might have different index publishing frequency.
        // Preferred frequency is once a week.
        if (true) {
            Instant updateStart = Instant.now();
            System.out.println("Updating Index...");
            System.out.println("This might take a while on first run, so please be patient!");

            Date centralContextCurrentTimestamp = centralContext.getTimestamp();
            IndexUpdateRequest updateRequest = new IndexUpdateRequest(centralContext, new Java11HttpClient());
            IndexUpdateResult updateResult = indexUpdater.fetchAndUpdateIndex(updateRequest);
            if (updateResult == null) {
                System.out.println("**updateResult is null** - assuming everything is up to date!");
            } else  if (updateResult.isFullUpdate()) {
                System.out.println("Full update happened!");
            } else if (centralContextCurrentTimestamp.equals(updateResult.getTimestamp())) {
                System.out.println("No update needed, index is up to date!");
            } else {
                System.out.println("Incremental update happened, change covered " + centralContextCurrentTimestamp
                        + " - " + updateResult.getTimestamp() + " period.");
            }

            System.out.println("Finished in "
                    + Duration.between(updateStart, Instant.now()).getSeconds() + " sec");
            System.out.println();
        }

        System.out.println();
        System.out.println("Using index");
        System.out.println("===========");
        System.out.println();

        final IndexSearcher searcher = centralContext.acquireIndexSearcher();
        Set<String> gavs = new HashSet<>();
        try {
            final IndexReader ir = searcher.getIndexReader();
            Bits liveDocs = MultiBits.getLiveDocs(ir);
            for (int i = 0; i < ir.maxDoc(); i++) {
                if (liveDocs == null || liveDocs.get(i)) {
                    final Document doc = ir.document(i);
                    final ArtifactInfo ai = IndexUtils.constructArtifactInfo(doc, centralContext);
                    if (ai == null) {
                        LOG.warning("ai is null!");
                    } else {
                        gavs.add(ai.getGroupId() + ":" + ai.getArtifactId() + ":" + ai.getVersion());
                    }
                }
            }
        } finally {
            centralContext.releaseIndexSearcher(searcher);
            // close cleanly
            indexer.closeIndexingContext(centralContext, false);
        }
        return gavs;
    }


    private static class Java11HttpClient implements ResourceFetcher {
        private final HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        private URI uri;

        @Override
        public void connect(String id, String url) throws IOException {
            this.uri = URI.create(url + "/");
        }

        @Override
        public void disconnect() throws IOException {}

        @Override
        public InputStream retrieve(String name) throws IOException, FileNotFoundException {
            HttpRequest request =
                    HttpRequest.newBuilder().uri(uri.resolve(name)).GET().build();
            try {
                HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
                if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                    return response.body();
                } else {
                    LOG.warning("request to " + request.uri() + "failed with response code " + response.statusCode());
                    LOG.warning("headers\n" + response.headers());
                    LOG.warning("body\n" + new String(response.body().readAllBytes(), StandardCharsets.UTF_8));
                    throw new IOException("Unexpected response for "+request.uri()+ " : " + response);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }
}