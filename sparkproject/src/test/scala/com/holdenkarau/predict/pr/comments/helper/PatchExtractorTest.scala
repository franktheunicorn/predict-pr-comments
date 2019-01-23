package com.holdenkarau.predict.pr.comments.sparkProject.helper
/**
 * Test of the Patch extractor
 */

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class PatchExtractorTest extends FunSuite {
  val simpleInput = """
From 97d57259eaf8ca29ce56a194de110d526c2d1629 Mon Sep 17 00:00:00 2001
From: songchenwen <me@songchenwen.com>
Date: Wed, 16 Jan 2019 19:12:23 +0800
Subject: [PATCH] Feature: SOURCE-IP-CIDR rule type

---
 README.md                  |  1 +
 adapters/inbound/http.go   |  4 +++-
 adapters/inbound/https.go  |  4 +++-
 adapters/inbound/socket.go |  1 +
 adapters/inbound/util.go   |  7 +++++++
 config/config.go           |  4 +++-
 constant/metadata.go       |  1 +
 constant/rule.go           |  3 +++
 rules/ipcidr.go            | 23 ++++++++++++++---------
 tunnel/tunnel.go           |  6 +++---
 10 files changed, 39 insertions(+), 15 deletions(-)

diff --git a/README.md b/README.md
index 31c2bb2..ec256ba 100644
--- a/README.md
+++ b/README.md
@@ -170,6 +170,7 @@ Rule:
 - DOMAIN,google.com,Proxy
 - DOMAIN-SUFFIX,ad.com,REJECT
 - IP-CIDR,127.0.0.0/8,DIRECT
+- SOURCE-IP-CIDR,192.168.1.201/32,DIRECT
 - GEOIP,CN,DIRECT
 # FINAL would remove after prerelease
 # you also can use `FINAL,Proxy` or `FINAL,,Proxy` now
diff --git a/adapters/inbound/http.go b/adapters/inbound/http.go
index 01aa14b..8aa21e7 100644
--- a/adapters/inbound/http.go
+++ b/adapters/inbound/http.go
@@ -32,8 +32,10 @@ func (h *HTTPAdapter) Conn() net.Conn {
 
 // NewHTTP is HTTPAdapter generator
 func NewHTTP(request *http.Request, conn net.Conn) *HTTPAdapter {
+	metadata := parseHTTPAddr(request)
+	metadata.SourceIP = parseSourceIP(conn)
 	return &HTTPAdapter{
-		metadata: parseHTTPAddr(request),
+		metadata: metadata,
 		R:        request,
 		conn:     conn,
 	}
"""

  test("Simple input") {
    val results = PatchExtractor.processPatch(simpleInput)
    val expected = List(
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        173,174,
        "- SOURCE-IP-CIDR,192.168.1.201/32,DIRECT",
        true),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        35,36,
	"	metadata := parseHTTPAddr(request)",
        true),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        35,37,
        "	metadata.SourceIP = parseSourceIP(conn)",
        true),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        37,38,
        "		metadata: parseHTTPAddr(request),",
        false),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        37,39,
        "		metadata: metadata,",
        true))
    results should contain theSameElementsAs expected
  }
}
