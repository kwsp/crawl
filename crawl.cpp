/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Web crawler based on curl and libxml2.
 * Copyright (C) 2018 - 2020 Jeroen Ooms <jeroenooms@gmail.com>
 * License: MIT
 *
 * Ported to C++ by Tiger Nie 2020
 * Added features
 *  - CLI interface
 *  - topology graph construction
 *  - prevent revisiting the same url more than once
 *
 * TODO:
 *  - Feature: NGraph does not allow multigraphs (e.g. two URIs point at each other)
 *  - Bug: URIs ending with/without a '/' should be treated the same
 *
 */

#include <cmath>
#include <csignal>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include <chrono>

#include <curl/curl.h>
#include <libxml/HTMLparser.h>
#include <libxml/uri.h>
#include <libxml/xpath.h>

#include "ngraph.hpp"

#define crawler_version "0.0.1"

using std::string;

/* Parameters and flags */
int max_con = 200;
int max_total = 20000;
int max_requests = 500;
size_t max_link_per_page = 20;
int follow_relative_links = 1;

const char *useragent =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/88.0.4292.0 Safari/537.36";

char *start_url = nullptr;

/* Network graph structure */
NGraph::tGraph<string> network;

/* Signal handlers */
int pending_interrupt = 0;
void sighandler(int dummy) {
  (void)dummy;
  pending_interrupt = 1;
}

//
//  libcurl write callback function
//
static int writer(char *data, size_t size, size_t nmemb, string *writerData) {
  if (!writerData)
    return 0;
  writerData->append(data, size * nmemb);
  return size * nmemb;
}

CURL *make_handle(char *url) {
  CURL *handle = curl_easy_init();

  /* Important: use HTTP2 over HTTPS */
  curl_easy_setopt(handle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
  curl_easy_setopt(handle, CURLOPT_URL, url);

  /* buffer body */
  string *buffer = new string;

  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writer);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, buffer);
  curl_easy_setopt(handle, CURLOPT_PRIVATE, buffer);

  /* For completeness */
  curl_easy_setopt(handle, CURLOPT_ACCEPT_ENCODING, "");
  curl_easy_setopt(handle, CURLOPT_TIMEOUT, 5L);
  curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(handle, CURLOPT_MAXREDIRS, 3L);
  curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT, 2L);
  curl_easy_setopt(handle, CURLOPT_COOKIEFILE, "");
  curl_easy_setopt(handle, CURLOPT_FILETIME, 1L);
  curl_easy_setopt(handle, CURLOPT_USERAGENT, useragent);
  curl_easy_setopt(handle, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
  curl_easy_setopt(handle, CURLOPT_UNRESTRICTED_AUTH, 1L);
  curl_easy_setopt(handle, CURLOPT_PROXYAUTH, CURLAUTH_ANY);
  curl_easy_setopt(handle, CURLOPT_EXPECT_100_TIMEOUT_MS, 0L);

  return handle;
}

/* HREF finder implemented in libxml2 but could be any HTML parser */
size_t follow_links(CURLM *multi_handle, string *mem, char *url) {
  // Only follow links from the start domain
  // TODO: This only allows link following if the url
  // begins with the start_url, so we start with
  // https://www.example.com/foo we won't follow
  // links from https://www.example.com/bar
  if (strncmp(url, start_url, strlen(start_url))) {
    return 0;
  }

  int opts = HTML_PARSE_NOBLANKS | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING |
             HTML_PARSE_NONET;
  htmlDocPtr doc = htmlReadMemory(mem->c_str(), mem->size(), url, NULL, opts);
  if (!doc)
    return 0;
  xmlChar *xpath = (xmlChar *)"//a/@href";
  xmlXPathContextPtr context = xmlXPathNewContext(doc);
  xmlXPathObjectPtr result = xmlXPathEvalExpression(xpath, context);
  xmlXPathFreeContext(context);
  if (!result)
    return 0;
  xmlNodeSetPtr nodeset = result->nodesetval;
  if (xmlXPathNodeSetIsEmpty(nodeset)) {
    xmlXPathFreeObject(result);
    return 0;
  }

  size_t count = 0;
  xmlURIPtr uri = xmlCreateURI();
  for (int i = 0; i < nodeset->nodeNr; i++) {
    const xmlNode *node = nodeset->nodeTab[i]->xmlChildrenNode;
    xmlChar *href = xmlNodeListGetString(doc, node, 1);
    if (follow_relative_links) {
      xmlChar *orig = href;
      href = xmlBuildURI(href, (xmlChar *)url);
      xmlFree(orig);
    }
    // remove fragment
    xmlParseURIReference(uri, (const char *)href);
    xmlFree(uri->fragment);
    uri->fragment = nullptr;
    char *link = (char *)xmlSaveUri(uri);
    if (!link || strlen(link) < 20)
      continue;
    if (!strncmp(link, "http://", 7) || !strncmp(link, "https://", 8)) {

      // If link has been visited already, skip adding to queue
      if (network.find(link) != network.end()) {
        network.insert_edge(url, link);
        continue;
      }
      network.insert_edge(url, link);

      curl_multi_add_handle(multi_handle, make_handle(link));
      if (count++ == max_link_per_page)
        break;
    }
    xmlFree(link);
  }
  xmlXPathFreeObject(result);
  xmlFreeURI(uri);
  return count;
}

int is_html(char *ctype) {
  return ctype != NULL && strlen(ctype) > 10 && strstr(ctype, "text/html");
}

void print_usage(char *pname) {
  fprintf(stderr, "Usage: %s [options...] <url>\n\
    -h                       Print this help text and exit\n\
    -v                       Verbose\n\
    -V, --version            Print version and exit\n\
    -c, --max-con <int>      Max # of simultaneously open connections in total (default %d)\n\
    -t, --max-total <int>    Max # of requests total (default %d)\n\
    -r, --max-requests <int> Max # of pending requests (default %d)\n\
    -m, --max-link-per-page  Max # of links to follow per page (default %zu)\n\
    -o, ---output <filename> Filename to write graphviz compatible network graph\n\
",
          pname, max_con, max_total, max_requests, max_link_per_page);
}

void print_version(char *pname) {
  fprintf(stderr, "%s %s\n", pname, crawler_version);
}

bool has_flag(const char *arg, const char *name1, const char *name2 = "") {
  return !strncmp(arg, name1, strlen(name1)) ||
         (strlen(name2) && !strncmp(arg, name2, strlen(name2)));
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    print_usage(argv[0]);
    std::exit(EXIT_FAILURE);
  }
  auto start = std::chrono::steady_clock::now();

  int verbose = 0;
  int i = 1;
  char *graphviz_fname = (char *)"out.gv";

  try {
    for (i = 1; i < argc; i++) {
      if (has_flag(argv[i], "-h")) {
        print_usage(argv[0]);
        std::exit(EXIT_SUCCESS);
      } else if (has_flag(argv[i], "-v")) {
        verbose = strlen(argv[i]) - 1;
      } else if (has_flag(argv[i], "-V", "--version")) {
        print_version(argv[0]);
        std::exit(EXIT_SUCCESS);
      } else if (has_flag(argv[i], "-c", "--max-con")) {
        max_con = std::stoi(argv[++i]);
      } else if (has_flag(argv[i], "-t", "--max-total")) {
        max_total = std::stoi(argv[++i]);
      } else if (has_flag(argv[i], "-r", "--max-requests")) {
        max_requests = std::stoi(argv[++i]);
      } else if (has_flag(argv[i], "-m", "--max-link-per-page")) {
        max_link_per_page = std::stoi(argv[++i]);
      } else if (has_flag(argv[i], "-o", "--output")) {
        graphviz_fname = argv[++i];
      } else if (i == argc-1) {
        start_url = argv[i];
      } else {
        fprintf(stderr, "Unknown flag: %s\n", argv[i]);
        std::exit(EXIT_FAILURE);
      }
    }
  } catch (std::out_of_range &err) {
    fprintf(stderr, "Invalid argument to %s\n", argv[i - 1]);
    std::exit(EXIT_FAILURE);
  } catch (std::invalid_argument &err) {
    fprintf(stderr, "Invalid argument to %s\n", argv[i - 1]);
    std::exit(EXIT_FAILURE);
  }

  if (start_url == nullptr) {
    fprintf(stderr, "%s: no URL specified!\n", argv[0]);
    std::exit(EXIT_FAILURE);
  }

  std::signal(SIGINT, sighandler);
  LIBXML_TEST_VERSION;
  curl_global_init(CURL_GLOBAL_DEFAULT);
  CURLM *multi_handle = curl_multi_init();
  curl_multi_setopt(multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, max_con);
  curl_multi_setopt(multi_handle, CURLMOPT_MAX_HOST_CONNECTIONS, 6L);

  /* enables http/2 if available */
#ifdef CURLPIPE_MULTIPLEX
  curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
#endif

  /* sets html start page */
  curl_multi_add_handle(multi_handle, make_handle(start_url));

  printf("Starting crawler at %s . . .\n", start_url);

  int msgs_left;
  int pending = 0;
  int complete = 0;
  std::vector<std::tuple<int, string> > broken_links;
  int still_running = 1;
  while (still_running && !pending_interrupt) {
    int numfds;
    curl_multi_wait(multi_handle, NULL, 0, 1000, &numfds);
    curl_multi_perform(multi_handle, &still_running);

    /* See how the transfers went */
    CURLMsg *m = NULL;
    while ((m = curl_multi_info_read(multi_handle, &msgs_left))) {
      if (m->msg == CURLMSG_DONE) {
        CURL *handle = m->easy_handle;
        char *url;
        string *mem;
        curl_easy_getinfo(handle, CURLINFO_PRIVATE, &mem);
        curl_easy_getinfo(handle, CURLINFO_EFFECTIVE_URL, &url);
        if (m->data.result == CURLE_OK) {
          long res_status;
          curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &res_status);
          if (res_status == 200) {
            char *ctype;
            curl_easy_getinfo(handle, CURLINFO_CONTENT_TYPE, &ctype);
            if (verbose > 0)
              printf("[%d] HTTP 200 (%s): %s\n", complete, ctype, url);
            if (is_html(ctype) && mem->size() > 100) {
              if (pending < max_requests && (complete + pending) < max_total) {
                pending += follow_links(multi_handle, mem, url);
                still_running = 1;
              }
            }
          } else {
            broken_links.push_back({(int)res_status, url});
            if (verbose > 0)
              printf("[%d] HTTP %d: %s\n", complete, (int)res_status, url);
          }
        } else {
          if (verbose > 0)
            printf("[%d] Connection failure: %s\n", complete, url);
        }
        curl_multi_remove_handle(multi_handle, handle);
        curl_easy_cleanup(handle);
        delete mem;
        complete++;
        pending--;
      }
    }
  }

  curl_multi_cleanup(multi_handle);
  curl_global_cleanup();

  /* print summary */
  if (int n_broken = broken_links.size()) {
    printf("\nSummary: %d/%d links are broken.\n", n_broken, complete);

    for (const auto &url : broken_links) {
      printf("  HTTP %d: %s\n", std::get<0>(url), std::get<1>(url).c_str());
    }
  } else {
    printf("\nSummary: checked %d links, no broken links found.\n", network.num_nodes());
  }
  if (verbose > 1) {
    printf("\n");
    network.print();
    printf("\n");
  }

  FILE *fptr = std::fopen(graphviz_fname, "w");
  if (fptr) {
    network.to_graphviz(fptr);
    printf("Wrote GraphViz output to %s\n", graphviz_fname);
    fclose(fptr);
  } else {
    fprintf(stderr, "Failed to write graphviz output to %s\n",
            graphviz_fname == nullptr ? "out.gv" : graphviz_fname);
    std::exit(EXIT_FAILURE);
  }
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff = end - start;
  printf("Took %.3fs\n", diff.count());

  return broken_links.size() ? EXIT_FAILURE : EXIT_SUCCESS;
}
