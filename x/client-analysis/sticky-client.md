FIRST

```
➜  build git:(dev) ✗ ./fwit-t --config-file prod.yaml --entities 50 --duration 10m --disable-chaos

 Successfully created 50 entities
📡 Starting 2 event subscribers...
📡 Subscriber 0 is listening to events from entity entity-33
📡 Subscriber 1 is listening to events from entity entity-31
🚀 Stress test started! Duration: 10m0s, Entities: 50 (Chaos Monkey Disabled)
{"time":"2025-06-20T15:56:37.18645145-04:00","level":"INFO","msg":"Attempting to connect to WebSocket for event subscription","service":"fwit-t","fwi-entities":{"entity-33":{"insi_client":{"url":"wss://red.insulalabs.io:443/db/api/v1/events/subscribe?topic=stress-topic"}}}}
{"time":"2025-06-20T15:56:37.186718572-04:00","level":"INFO","msg":"Attempting to connect to WebSocket for event subscription","service":"fwit-t","fwi-entities":{"entity-31":{"insi_client":{"url":"wss://blue.insulalabs.io:443/db/api/v1/events/subscribe?topic=stress-topic"}}}}
{"time":"2025-06-20T15:56:37.426526293-04:00","level":"INFO","msg":"Successfully connected to WebSocket. Listening for events...","service":"fwit-t","fwi-entities":{"entity-33":{"insi_client":{"topic":"stress-topic"}}}}
{"time":"2025-06-20T15:56:37.672761586-04:00","level":"INFO","msg":"Successfully connected to WebSocket. Listening for events...","service":"fwit-t","fwi-entities":{"entity-31":{"insi_client":{"topic":"stress-topic"}}}}
{"time":"2025-06-20T16:06:37.186438235-04:00","level":"INFO","msg":"Context cancelled, closing WebSocket ping loop.","service":"fwit-t"}
{"time":"2025-06-20T16:06:37.186561802-04:00","level":"INFO","msg":"Context cancelled, closing WebSocket ping loop.","service":"fwit-t"}

🏁 Stress test duration completed.
{"time":"2025-06-20T16:06:37.22630619-04:00","level":"INFO","msg":"WebSocket connection closed gracefully or context cancelled.","service":"fwit-t","fwi-entities":{"entity-31":{"insi_client":{"error":"websocket: close 1000 (normal)"}}}}
{"time":"2025-06-20T16:06:37.226322201-04:00","level":"INFO","msg":"WebSocket connection closed gracefully or context cancelled.","service":"fwit-t","fwi-entities":{"entity-33":{"insi_client":{"error":"websocket: close 1000 (normal)"}}}}
⚠️ Subscriber 1 exited with error: websocket: close 1000 (normal)
⚠️ Subscriber 0 exited with error: websocket: close 1000 (normal)
✅ Stress test finished.

--- 📊 Stress Test Summary ---
Entity          | Sets (avg)       | Gets (avg)       | Deletes (avg)    | Caches (avg)     | Publishes (avg)  | Bumps (avg)      | Blob Sets (avg)  | Blob Gets (avg)  | Blob Dels (avg)  | Failures  
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
entity-12       | 294   (270.966655ms) | 277   (44.337157ms) | 289   (253.70482ms) | 284   (246.138869ms) | 284   (258.692898ms) | 328   (260.80329ms) | 276   (306.939695ms) | 295   (44.336618ms) | 264   (258.7474ms) | 0         
entity-3        | 374   (215.723597ms) | 318   (90.908971ms) | 331   (216.394473ms) | 302   (212.657363ms) | 325   (220.550799ms) | 319   (209.7098ms) | 305   (270.504169ms) | 279   (96.329759ms) | 283   (222.47342ms) | 0         
entity-11       | 347   (202.702673ms) | 300   (82.413081ms) | 327   (203.965644ms) | 359   (204.224736ms) | 337   (198.970968ms) | 355   (203.385881ms) | 350   (269.880623ms) | 319   (76.504266ms) | 284   (201.054714ms) | 0         
entity-46       | 285   (261.143589ms) | 244   (43.733572ms) | 283   (249.893948ms) | 295   (253.534283ms) | 289   (257.559138ms) | 300   (261.929557ms) | 305   (306.936477ms) | 300   (43.165182ms) | 276   (254.132365ms) | 0         
entity-36       | 321   (247.938983ms) | 270   (43.528129ms) | 270   (251.002972ms) | 310   (261.443494ms) | 288   (255.591569ms) | 291   (264.028957ms) | 268   (334.34037ms) | 277   (44.973059ms) | 265   (258.712887ms) | 0         
entity-28       | 265   (258.07776ms) | 263   (46.751355ms) | 257   (252.695095ms) | 296   (252.335485ms) | 306   (255.43411ms) | 302   (256.40612ms) | 292   (332.509112ms) | 273   (42.402349ms) | 290   (260.622804ms) | 0         
entity-13       | 315   (255.230872ms) | 288   (43.599137ms) | 278   (251.039263ms) | 274   (262.531323ms) | 306   (262.920741ms) | 322   (243.897916ms) | 280   (323.357278ms) | 261   (40.449031ms) | 259   (252.376964ms) | 0         
entity-15       | 281   (260.2679ms) | 285   (45.690949ms) | 256   (251.661977ms) | 304   (260.66754ms) | 312   (247.647042ms) | 280   (265.013166ms) | 297   (309.178593ms) | 301   (45.39796ms) | 284   (257.526863ms) | 0         
entity-20       | 276   (257.948893ms) | 279   (40.916575ms) | 270   (260.302401ms) | 312   (257.351554ms) | 306   (259.916102ms) | 258   (246.208775ms) | 306   (324.440931ms) | 307   (44.244342ms) | 281   (254.106485ms) | 0         
entity-30       | 288   (251.637539ms) | 268   (38.955038ms) | 274   (256.752794ms) | 257   (246.952938ms) | 310   (271.601367ms) | 311   (246.258969ms) | 309   (318.017733ms) | 265   (45.373617ms) | 292   (249.190645ms) | 1         
entity-18       | 306   (253.804244ms) | 313   (44.070181ms) | 281   (252.304079ms) | 302   (255.691311ms) | 328   (252.357784ms) | 329   (250.896403ms) | 250   (338.397154ms) | 186   (43.87408ms) | 239   (254.288674ms) | 0         
entity-6        | 323   (210.732075ms) | 311   (103.736262ms) | 309   (215.820337ms) | 313   (209.579813ms) | 294   (219.479718ms) | 306   (211.584446ms) | 350   (284.035601ms) | 326   (91.051073ms) | 301   (219.13237ms) | 1         
entity-40       | 301   (255.421991ms) | 288   (43.256298ms) | 298   (258.73638ms) | 282   (256.089211ms) | 307   (248.032145ms) | 279   (262.7241ms) | 289   (324.693087ms) | 250   (43.257754ms) | 273   (249.379236ms) | 0         
entity-8        | 334   (212.518438ms) | 307   (90.803506ms) | 307   (212.081956ms) | 319   (215.437062ms) | 331   (214.484115ms) | 319   (220.985101ms) | 333   (281.750433ms) | 299   (95.399002ms) | 264   (219.700078ms) | 1         
entity-21       | 279   (273.646726ms) | 306   (46.081462ms) | 277   (252.938821ms) | 258   (243.239198ms) | 328   (255.790389ms) | 282   (265.087966ms) | 289   (331.335809ms) | 274   (43.312208ms) | 275   (258.035019ms) | 0         
entity-42       | 339   (198.369262ms) | 333   (82.503914ms) | 310   (209.982094ms) | 343   (205.43361ms) | 337   (202.04213ms) | 360   (197.838179ms) | 340   (273.022861ms) | 301   (76.64442ms) | 337   (201.56209ms) | 1         
entity-4        | 326   (218.638666ms) | 288   (95.471269ms) | 318   (212.458501ms) | 352   (202.800551ms) | 314   (214.505764ms) | 316   (216.531668ms) | 303   (293.459072ms) | 320   (94.180511ms) | 294   (214.106873ms) | 0         
entity-45       | 277   (262.134204ms) | 251   (40.137107ms) | 249   (244.000927ms) | 295   (252.449443ms) | 324   (253.296117ms) | 295   (252.022466ms) | 299   (322.996896ms) | 311   (43.613219ms) | 282   (267.966124ms) | 0         
entity-49       | 363   (199.57258ms) | 299   (82.598458ms) | 345   (209.530838ms) | 309   (207.695696ms) | 310   (211.454753ms) | 357   (196.910652ms) | 365   (264.289274ms) | 351   (82.043258ms) | 311   (191.028461ms) | 1         
entity-27       | 283   (259.281729ms) | 300   (39.778108ms) | 265   (258.784262ms) | 309   (254.606348ms) | 273   (257.722317ms) | 288   (247.326591ms) | 291   (342.813049ms) | 309   (48.960974ms) | 279   (253.77796ms) | 1         
entity-39       | 344   (206.336139ms) | 298   (82.577019ms) | 316   (201.563519ms) | 344   (193.974904ms) | 336   (195.774551ms) | 378   (202.020793ms) | 352   (267.743449ms) | 329   (80.417818ms) | 328   (197.687678ms) | 1         
entity-5        | 333   (220.655784ms) | 284   (93.591897ms) | 319   (212.453689ms) | 301   (220.335577ms) | 287   (213.216036ms) | 338   (212.24533ms) | 320   (282.979744ms) | 343   (96.717442ms) | 305   (215.22257ms) | 0         
entity-9        | 336   (219.718086ms) | 327   (99.16736ms) | 323   (210.410563ms) | 324   (214.672626ms) | 310   (222.809124ms) | 306   (208.633985ms) | 319   (280.816047ms) | 313   (93.370103ms) | 288   (214.820202ms) | 0         
entity-23       | 304   (259.867132ms) | 278   (42.254097ms) | 295   (250.997663ms) | 298   (257.691308ms) | 323   (253.510888ms) | 264   (253.230388ms) | 305   (313.721609ms) | 272   (43.073091ms) | 254   (249.988661ms) | 0         
entity-7        | 298   (212.84058ms) | 295   (98.787964ms) | 289   (213.259222ms) | 300   (220.907007ms) | 334   (218.7971ms) | 338   (214.707266ms) | 322   (279.787276ms) | 323   (95.426818ms) | 322   (218.380062ms) | 0         
entity-26       | 291   (258.532601ms) | 273   (43.6067ms) | 268   (253.183332ms) | 269   (247.199714ms) | 277   (265.946889ms) | 294   (261.892901ms) | 310   (317.137855ms) | 264   (44.504417ms) | 297   (264.656008ms) | 0         
entity-24       | 296   (258.344064ms) | 260   (42.644285ms) | 288   (254.646168ms) | 247   (267.944111ms) | 270   (262.491972ms) | 311   (248.84717ms) | 308   (315.526858ms) | 288   (46.608513ms) | 283   (255.070863ms) | 0         
entity-47       | 363   (200.173517ms) | 347   (80.977092ms) | 331   (205.567336ms) | 343   (190.555461ms) | 346   (191.843753ms) | 341   (202.876013ms) | 340   (268.171277ms) | 338   (81.048136ms) | 329   (197.230922ms) | 0         
entity-38       | 353   (200.807679ms) | 321   (80.899109ms) | 340   (195.887396ms) | 349   (204.750849ms) | 328   (210.589961ms) | 346   (204.201804ms) | 341   (265.774133ms) | 318   (80.664654ms) | 317   (196.86928ms) | 0         
entity-34       | 320   (248.779561ms) | 299   (42.68733ms) | 298   (248.048904ms) | 299   (251.069836ms) | 280   (244.901057ms) | 304   (248.378501ms) | 285   (320.179201ms) | 270   (40.131749ms) | 278   (258.316173ms) | 0         
entity-19       | 350   (203.246312ms) | 327   (75.923694ms) | 327   (200.276578ms) | 360   (202.566144ms) | 336   (204.593589ms) | 354   (197.144776ms) | 343   (264.945097ms) | 315   (83.171921ms) | 316   (199.926462ms) | 0         
entity-2        | 299   (212.218445ms) | 299   (95.508124ms) | 296   (214.795405ms) | 325   (210.680676ms) | 324   (217.82317ms) | 332   (221.678723ms) | 320   (279.867634ms) | 311   (96.415111ms) | 308   (221.982533ms) | 0         
entity-16       | 276   (245.991061ms) | 261   (42.338502ms) | 272   (251.265312ms) | 314   (259.749613ms) | 275   (251.574106ms) | 280   (250.928112ms) | 308   (343.055049ms) | 316   (42.764273ms) | 271   (255.963504ms) | 0         
entity-35       | 294   (254.94528ms) | 256   (42.656826ms) | 265   (245.967713ms) | 300   (245.627957ms) | 296   (257.446622ms) | 297   (252.647947ms) | 308   (316.551439ms) | 287   (44.51627ms) | 293   (255.421607ms) | 0         
entity-33       | 269   (258.330729ms) | 243   (42.117383ms) | 269   (255.498006ms) | 301   (251.072034ms) | 284   (260.900581ms) | 310   (254.475435ms) | 293   (327.900054ms) | 293   (43.869594ms) | 284   (258.632916ms) | 0         
entity-31       | 281   (251.413849ms) | 273   (44.681581ms) | 276   (256.498596ms) | 256   (257.972008ms) | 302   (259.264664ms) | 289   (253.07105ms) | 310   (331.578596ms) | 266   (43.36568ms) | 301   (251.826841ms) | 0         
entity-14       | 289   (260.463032ms) | 277   (44.989565ms) | 269   (262.337303ms) | 297   (255.609388ms) | 287   (253.624743ms) | 298   (244.538739ms) | 292   (319.256953ms) | 277   (43.948955ms) | 271   (261.371307ms) | 0         
entity-37       | 297   (262.373854ms) | 261   (43.279481ms) | 287   (257.684146ms) | 292   (249.875727ms) | 317   (253.623024ms) | 290   (250.461319ms) | 271   (333.860669ms) | 256   (41.239171ms) | 271   (255.943574ms) | 0         
entity-17       | 298   (263.771343ms) | 281   (46.153271ms) | 269   (250.841029ms) | 298   (251.95058ms) | 301   (262.993944ms) | 307   (248.813253ms) | 293   (311.534905ms) | 288   (42.843474ms) | 272   (246.870869ms) | 0         
entity-1        | 327   (218.580503ms) | 277   (93.101702ms) | 301   (216.175198ms) | 314   (213.214858ms) | 319   (219.088456ms) | 352   (217.233638ms) | 319   (281.887816ms) | 277   (93.124477ms) | 307   (212.667019ms) | 0         
entity-0        | 303   (215.371603ms) | 312   (93.575117ms) | 300   (218.709204ms) | 334   (221.793463ms) | 320   (216.81022ms) | 315   (217.306906ms) | 325   (277.058588ms) | 311   (100.818003ms) | 305   (210.069803ms) | 0         
entity-22       | 285   (249.514886ms) | 268   (42.007438ms) | 283   (258.947866ms) | 306   (258.676838ms) | 282   (262.397957ms) | 317   (256.922781ms) | 279   (322.026992ms) | 244   (41.310118ms) | 277   (253.119139ms) | 0         
entity-43       | 275   (249.689865ms) | 290   (42.211851ms) | 272   (252.547608ms) | 277   (264.664305ms) | 284   (251.084898ms) | 278   (259.107915ms) | 316   (331.261086ms) | 269   (50.278228ms) | 294   (257.012791ms) | 0         
entity-41       | 333   (212.047457ms) | 308   (85.451136ms) | 316   (187.813479ms) | 353   (202.971919ms) | 342   (201.303201ms) | 330   (202.187954ms) | 352   (267.00641ms) | 339   (81.843631ms) | 341   (202.75383ms) | 0         
entity-29       | 342   (201.432966ms) | 316   (81.5687ms) | 330   (200.112738ms) | 360   (203.920056ms) | 350   (194.989685ms) | 349   (206.437256ms) | 339   (269.008773ms) | 306   (83.072373ms) | 318   (199.151182ms) | 0         
entity-48       | 290   (243.842069ms) | 294   (38.403721ms) | 272   (247.550144ms) | 276   (258.914266ms) | 313   (252.945626ms) | 295   (262.323218ms) | 297   (325.56809ms) | 290   (41.069067ms) | 290   (257.546775ms) | 0         
entity-32       | 284   (247.141461ms) | 259   (44.140828ms) | 276   (250.108709ms) | 265   (254.165692ms) | 301   (260.008525ms) | 300   (262.33828ms) | 298   (330.439813ms) | 280   (42.883616ms) | 289   (260.16661ms) | 0         
entity-25       | 297   (258.649504ms) | 289   (45.017109ms) | 270   (248.602472ms) | 285   (260.056831ms) | 301   (261.061507ms) | 292   (262.507902ms) | 300   (316.464264ms) | 259   (48.572424ms) | 264   (253.693725ms) | 0         
entity-10       | 295   (263.952382ms) | 283   (39.990064ms) | 261   (251.436661ms) | 318   (255.424916ms) | 312   (260.717387ms) | 295   (266.414217ms) | 266   (312.12136ms) | 274   (44.608433ms) | 265   (261.780579ms) | 0         
entity-44       | 315   (249.492562ms) | 232   (46.322109ms) | 268   (245.605692ms) | 296   (260.701554ms) | 263   (260.662198ms) | 262   (268.433056ms) | 315   (331.03854ms) | 265   (43.638223ms) | 291   (255.745473ms) | 0         
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTALS          | 15414 (236.948246ms) | 14376 (61.731462ms) | 14540 (234.251697ms) | 15276 (235.724755ms) | 15409 (238.056661ms) | 15559 (236.147333ms) | 15443 (303.509394ms) | 14585 (62.221804ms) | 14462 (236.641454ms) | 7         

--- 📡 Subscriber Summary ---
📡 Subscriber 0 (listening to entity-33) received 284 events
📡 Subscriber 1 (listening to entity-31) received 302 events
📡 Total Events Received by All Subscribers: 586

--- ❗ Detailed Error Log ---
[2025-06-20T16:06:37-04:00] Entity 'entity-6': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:06:37-04:00] Entity 'entity-39': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:06:37-04:00] Entity 'entity-30': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:06:37-04:00] Entity 'entity-8': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:06:37-04:00] Entity 'entity-49': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:06:37-04:00] Entity 'entity-27': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:06:37-04:00] Entity 'entity-42': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded

✅ Stress test on external cluster complete.
➜  build git:(dev) ✗ 
```


# AFTER CLIENT UPDATE (With sticky leader)


```
./fwit-t --config-file prod.yaml --entities 50 --duration 10m --disable-chaos
🔎 Using existing cluster configuration from: prod.yaml
✅ Successfully loaded configuration for external cluster.
{"time":"2025-06-20T16:16:35.943408804-04:00","level":"INFO","msg":"FWI initialized","service":"fwit-t","fwi":{"loaded_entities":54}}
👥 Creating 50 entities in parallel (one goroutine per entity)...
✅ Successfully created 50 entities
📡 Starting 2 event subscribers...
📡 Subscriber 0 is listening to events from entity entity-33
📡 Subscriber 1 is listening to events from entity entity-25
🚀 Stress test started! Duration: 10m0s, Entities: 50 (Chaos Monkey Disabled)
{"time":"2025-06-20T16:16:35.944045935-04:00","level":"INFO","msg":"Attempting to connect to WebSocket for event subscription","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-33":{"url":"wss://green.insulalabs.io:443/db/api/v1/events/subscribe?topic=stress-topic"}}}}
{"time":"2025-06-20T16:16:35.94424719-04:00","level":"INFO","msg":"Attempting to connect to WebSocket for event subscription","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-25":{"url":"wss://green.insulalabs.io:443/db/api/v1/events/subscribe?topic=stress-topic"}}}}
{"time":"2025-06-20T16:16:36.118089626-04:00","level":"INFO","msg":"Successfully connected to WebSocket. Listening for events...","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-25":{"topic":"stress-topic"}}}}
{"time":"2025-06-20T16:16:36.152654028-04:00","level":"INFO","msg":"Successfully connected to WebSocket. Listening for events...","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-33":{"topic":"stress-topic"}}}}
{"time":"2025-06-20T16:26:35.944101709-04:00","level":"INFO","msg":"Context cancelled, closing WebSocket ping loop.","service":"fwit-t"}
{"time":"2025-06-20T16:26:35.944143021-04:00","level":"INFO","msg":"Context cancelled, closing WebSocket ping loop.","service":"fwit-t"}

🏁 Stress test duration completed.
{"time":"2025-06-20T16:26:35.983517196-04:00","level":"INFO","msg":"Context cancelled, closing WebSocket read loop.","service":"fwit-t"}
{"time":"2025-06-20T16:26:35.983616913-04:00","level":"ERROR","msg":"Error sending close message during read loop shutdown","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-25":{"error":"websocket: close sent"}}}}
⚠️ Subscriber 1 exited with error: context deadline exceeded
{"time":"2025-06-20T16:26:36.122239832-04:00","level":"INFO","msg":"WebSocket connection closed gracefully or context cancelled.","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-33":{"error":"websocket: close 1000 (normal)"}}}}
⚠️ Subscriber 0 exited with error: websocket: close 1000 (normal)
✅ Stress test finished.

--- 📊 Stress Test Summary ---
Entity          | Sets (avg)       | Gets (avg)       | Deletes (avg)    | Caches (avg)     | Publishes (avg)  | Bumps (avg)      | Blob Sets (avg)  | Blob Gets (avg)  | Blob Dels (avg)  | Failures  
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
entity-24       | 233   (303.697126ms) | 216   (130.992487ms) | 207   (305.45746ms) | 236   (298.625355ms) | 246   (306.539721ms) | 240   (312.210643ms) | 221   (402.36458ms) | 222   (133.28612ms) | 213   (306.946653ms) | 1         
entity-17       | 253   (311.800982ms) | 254   (126.425743ms) | 228   (313.156035ms) | 233   (302.032929ms) | 205   (319.34069ms) | 248   (311.106994ms) | 208   (384.937009ms) | 209   (129.252702ms) | 207   (315.873659ms) | 1         
entity-43       | 220   (310.876133ms) | 191   (128.402992ms) | 211   (303.468276ms) | 243   (313.629023ms) | 233   (312.521015ms) | 211   (310.908368ms) | 234   (390.021668ms) | 240   (123.657577ms) | 233   (322.180184ms) | 1         
entity-14       | 241   (308.9239ms) | 200   (131.603041ms) | 234   (310.640343ms) | 215   (302.658245ms) | 236   (307.599819ms) | 229   (304.612257ms) | 240   (398.459431ms) | 219   (133.78251ms) | 198   (306.186375ms) | 1         
entity-47       | 208   (313.638989ms) | 199   (128.775263ms) | 201   (312.011966ms) | 209   (305.830473ms) | 253   (306.936205ms) | 251   (310.206071ms) | 238   (393.97787ms) | 216   (129.013394ms) | 234   (309.644499ms) | 1         
entity-36       | 227   (307.324269ms) | 199   (130.581597ms) | 219   (308.449513ms) | 234   (303.103566ms) | 200   (300.89454ms) | 245   (304.612905ms) | 248   (401.932615ms) | 234   (122.184336ms) | 234   (304.154524ms) | 1         
entity-34       | 225   (293.483716ms) | 200   (133.226546ms) | 211   (312.031275ms) | 227   (319.481855ms) | 226   (303.219378ms) | 246   (313.413383ms) | 241   (396.000283ms) | 240   (123.737041ms) | 217   (306.872306ms) | 1         
entity-28       | 220   (309.727183ms) | 224   (130.938041ms) | 216   (308.68811ms) | 235   (307.59199ms) | 231   (306.13582ms) | 230   (302.508046ms) | 244   (386.862169ms) | 210   (124.884281ms) | 217   (323.55346ms) | 1         
entity-40       | 245   (310.599218ms) | 223   (126.01808ms) | 220   (308.237205ms) | 249   (307.363034ms) | 245   (302.662643ms) | 229   (302.541289ms) | 225   (394.902953ms) | 167   (120.947051ms) | 224   (300.678693ms) | 1         
entity-23       | 243   (296.668222ms) | 218   (129.592637ms) | 227   (316.374254ms) | 245   (312.696059ms) | 188   (299.27259ms) | 216   (300.987946ms) | 252   (408.227335ms) | 247   (128.41645ms) | 210   (307.703326ms) | 0         
entity-33       | 218   (318.416624ms) | 190   (129.134142ms) | 202   (304.350825ms) | 224   (317.03471ms) | 232   (307.761853ms) | 283   (310.389787ms) | 224   (393.587723ms) | 220   (126.612413ms) | 218   (302.411428ms) | 1         
entity-21       | 247   (314.957315ms) | 228   (122.087229ms) | 227   (298.353931ms) | 248   (308.694529ms) | 216   (318.187219ms) | 229   (308.103647ms) | 233   (387.985445ms) | 194   (124.413079ms) | 214   (304.041004ms) | 1         
entity-6        | 220   (303.322802ms) | 228   (125.642775ms) | 213   (306.476056ms) | 224   (313.440274ms) | 244   (307.253071ms) | 249   (301.936774ms) | 227   (402.475305ms) | 251   (122.573127ms) | 199   (315.300469ms) | 1         
entity-10       | 247   (302.505475ms) | 253   (119.916852ms) | 226   (302.539202ms) | 228   (301.682555ms) | 215   (316.348556ms) | 269   (306.157712ms) | 205   (403.838507ms) | 228   (129.319433ms) | 205   (307.837434ms) | 0         
entity-30       | 216   (317.602108ms) | 231   (129.852195ms) | 203   (309.296506ms) | 251   (307.694944ms) | 242   (315.09882ms) | 213   (299.629448ms) | 245   (394.988404ms) | 215   (127.876811ms) | 215   (306.888998ms) | 0         
entity-7        | 231   (303.393477ms) | 242   (128.62845ms) | 217   (303.621211ms) | 264   (317.864137ms) | 249   (311.702519ms) | 226   (323.143733ms) | 211   (399.363958ms) | 169   (129.147629ms) | 200   (299.776097ms) | 0         
entity-1        | 254   (308.324134ms) | 236   (128.810329ms) | 221   (317.15538ms) | 221   (295.266345ms) | 232   (304.677622ms) | 230   (313.230308ms) | 217   (391.023526ms) | 215   (131.966533ms) | 217   (316.722653ms) | 0         
entity-0        | 225   (310.795615ms) | 207   (128.585529ms) | 218   (304.965619ms) | 240   (309.373847ms) | 241   (311.105729ms) | 224   (303.773659ms) | 233   (394.153378ms) | 221   (125.55231ms) | 231   (296.880405ms) | 0         
entity-9        | 229   (311.518765ms) | 202   (129.991531ms) | 214   (291.552208ms) | 220   (314.755221ms) | 270   (310.406946ms) | 201   (312.703824ms) | 255   (404.258536ms) | 216   (127.945212ms) | 206   (303.973901ms) | 0         
entity-35       | 223   (310.325537ms) | 239   (135.571759ms) | 197   (303.228529ms) | 256   (306.815745ms) | 245   (305.72817ms) | 254   (306.346501ms) | 214   (380.977352ms) | 234   (128.208046ms) | 208   (303.059625ms) | 0         
entity-46       | 250   (302.964219ms) | 200   (128.305735ms) | 247   (308.460627ms) | 221   (316.96105ms) | 250   (311.031473ms) | 229   (315.98658ms) | 214   (393.023337ms) | 181   (126.611117ms) | 209   (306.734901ms) | 0         
entity-4        | 202   (312.612365ms) | 185   (129.37127ms) | 198   (293.180747ms) | 249   (306.17709ms) | 235   (332.522779ms) | 235   (307.825633ms) | 243   (399.998524ms) | 263   (123.724249ms) | 211   (299.698574ms) | 0         
entity-16       | 227   (308.452824ms) | 226   (128.32277ms) | 211   (309.820754ms) | 238   (311.278021ms) | 244   (312.903131ms) | 244   (307.045212ms) | 211   (398.912842ms) | 223   (134.834416ms) | 202   (317.735316ms) | 0         
entity-8        | 236   (305.994743ms) | 227   (125.36807ms) | 235   (294.313056ms) | 228   (321.583217ms) | 232   (308.260736ms) | 247   (298.756466ms) | 234   (395.656236ms) | 229   (126.749432ms) | 193   (303.755609ms) | 0         
entity-27       | 232   (309.382074ms) | 226   (131.670475ms) | 205   (305.989273ms) | 228   (311.493373ms) | 233   (291.953849ms) | 264   (301.197596ms) | 234   (386.677929ms) | 201   (125.139415ms) | 221   (314.327102ms) | 0         
entity-19       | 252   (312.366892ms) | 231   (125.921108ms) | 227   (295.532463ms) | 229   (303.987826ms) | 225   (304.414925ms) | 202   (311.589048ms) | 251   (383.133474ms) | 222   (124.983862ms) | 223   (309.223822ms) | 0         
entity-31       | 234   (318.612979ms) | 174   (127.958673ms) | 226   (303.003428ms) | 238   (323.642ms) | 254   (312.180373ms) | 231   (304.200405ms) | 220   (387.010399ms) | 188   (124.635193ms) | 217   (308.506127ms) | 0         
entity-15       | 242   (320.030795ms) | 213   (126.495549ms) | 227   (303.709976ms) | 238   (301.715671ms) | 230   (297.836748ms) | 237   (301.97839ms) | 229   (403.691436ms) | 190   (128.416342ms) | 220   (304.978556ms) | 0         
entity-39       | 242   (307.435896ms) | 212   (126.757322ms) | 225   (303.199304ms) | 221   (310.953234ms) | 267   (298.849702ms) | 217   (303.028181ms) | 240   (403.094306ms) | 219   (135.727143ms) | 191   (308.219416ms) | 0         
entity-26       | 247   (315.956094ms) | 200   (126.21862ms) | 236   (304.637539ms) | 216   (312.86544ms) | 220   (304.132812ms) | 221   (315.040125ms) | 233   (408.115216ms) | 213   (133.648173ms) | 221   (302.02262ms) | 0         
entity-25       | 225   (312.977163ms) | 221   (128.886678ms) | 218   (313.362122ms) | 222   (302.33006ms) | 236   (302.634168ms) | 231   (319.233382ms) | 239   (397.805285ms) | 232   (128.031093ms) | 222   (289.193327ms) | 0         
entity-22       | 261   (305.196403ms) | 233   (124.174343ms) | 230   (303.5369ms) | 218   (310.332187ms) | 232   (305.273296ms) | 221   (315.148238ms) | 219   (390.772661ms) | 221   (125.13518ms) | 216   (319.337366ms) | 0         
entity-49       | 235   (308.231469ms) | 225   (129.95857ms) | 224   (306.71187ms) | 250   (312.335151ms) | 257   (304.13597ms) | 212   (298.877031ms) | 231   (393.083515ms) | 196   (126.757861ms) | 208   (299.951567ms) | 0         
entity-13       | 229   (308.566208ms) | 196   (127.421995ms) | 222   (311.098958ms) | 239   (310.930375ms) | 225   (305.441802ms) | 230   (301.143549ms) | 229   (408.753855ms) | 225   (123.251919ms) | 223   (317.660324ms) | 0         
entity-12       | 230   (315.429403ms) | 222   (130.06387ms) | 206   (286.949958ms) | 217   (324.319127ms) | 241   (314.419339ms) | 250   (309.730101ms) | 233   (395.086056ms) | 221   (124.417493ms) | 208   (314.107646ms) | 0         
entity-32       | 241   (302.433533ms) | 213   (126.072975ms) | 241   (307.21119ms) | 238   (297.950988ms) | 236   (303.925472ms) | 248   (306.466274ms) | 223   (413.226937ms) | 200   (123.751663ms) | 199   (300.466126ms) | 0         
entity-42       | 242   (319.787228ms) | 210   (124.635315ms) | 220   (303.60262ms) | 225   (311.037134ms) | 236   (308.888585ms) | 213   (308.404445ms) | 243   (385.519901ms) | 223   (121.302631ms) | 225   (310.324691ms) | 0         
entity-5        | 234   (306.556646ms) | 220   (128.371794ms) | 214   (307.423924ms) | 209   (315.093102ms) | 227   (308.919196ms) | 248   (314.69921ms) | 236   (380.411921ms) | 247   (129.81058ms) | 202   (317.098353ms) | 0         
entity-41       | 220   (310.818481ms) | 190   (127.968224ms) | 199   (310.072892ms) | 220   (311.877788ms) | 261   (307.485383ms) | 221   (312.472808ms) | 242   (390.423845ms) | 229   (127.75148ms) | 232   (313.859696ms) | 0         
entity-38       | 227   (302.989858ms) | 216   (123.920308ms) | 213   (303.00988ms) | 221   (321.711204ms) | 231   (310.58967ms) | 221   (307.019163ms) | 249   (394.559636ms) | 236   (128.027438ms) | 226   (305.929179ms) | 0         
entity-2        | 216   (309.0668ms) | 183   (127.030859ms) | 213   (312.565757ms) | 236   (308.321666ms) | 250   (301.351983ms) | 260   (315.50231ms) | 221   (394.857354ms) | 211   (126.874688ms) | 215   (310.268475ms) | 0         
entity-3        | 250   (310.406146ms) | 228   (127.122929ms) | 222   (300.022178ms) | 222   (311.634708ms) | 207   (307.6167ms) | 223   (314.287141ms) | 256   (389.344402ms) | 199   (123.472789ms) | 225   (304.149739ms) | 0         
entity-11       | 218   (310.486544ms) | 200   (126.26878ms) | 211   (303.369319ms) | 247   (308.598721ms) | 221   (305.376866ms) | 227   (307.153194ms) | 251   (397.414119ms) | 222   (128.621185ms) | 228   (307.717182ms) | 0         
entity-20       | 239   (308.144687ms) | 219   (130.821713ms) | 234   (306.189466ms) | 220   (316.055191ms) | 224   (302.904166ms) | 236   (306.621421ms) | 244   (389.340883ms) | 229   (121.207169ms) | 206   (304.004741ms) | 0         
entity-48       | 268   (299.862771ms) | 212   (136.807041ms) | 257   (299.67792ms) | 231   (308.412786ms) | 228   (313.657633ms) | 238   (304.35693ms) | 202   (394.890546ms) | 222   (129.534837ms) | 179   (320.587889ms) | 0         
entity-37       | 250   (310.877328ms) | 217   (130.349673ms) | 225   (305.80793ms) | 238   (308.206357ms) | 230   (313.011367ms) | 217   (303.67313ms) | 224   (396.971104ms) | 216   (128.763153ms) | 216   (308.648907ms) | 0         
entity-44       | 217   (302.058399ms) | 214   (121.350736ms) | 196   (304.765955ms) | 236   (321.769821ms) | 248   (307.319963ms) | 234   (303.401888ms) | 245   (393.523902ms) | 218   (137.206186ms) | 226   (306.916642ms) | 0         
entity-29       | 237   (318.565868ms) | 218   (131.698404ms) | 232   (303.581323ms) | 234   (308.339039ms) | 237   (318.277158ms) | 226   (293.093703ms) | 224   (386.426888ms) | 240   (124.576161ms) | 205   (313.428354ms) | 0         
entity-45       | 229   (305.799762ms) | 232   (126.821087ms) | 225   (302.358847ms) | 217   (312.099196ms) | 245   (304.473268ms) | 248   (314.381097ms) | 223   (405.606988ms) | 217   (120.60218ms) | 214   (297.869671ms) | 0         
entity-18       | 221   (316.774245ms) | 211   (121.903167ms) | 210   (310.84612ms) | 261   (304.639947ms) | 218   (297.01694ms) | 260   (299.277417ms) | 227   (410.017836ms) | 236   (135.596967ms) | 198   (307.539856ms) | 0         
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTALS          | 11678 (309.281121ms) | 10754 (128.015309ms) | 10961 (305.275644ms) | 11609 (310.192584ms) | 11729 (307.820528ms) | 11714 (307.596202ms) | 11585 (395.545963ms) | 10936 (127.25242ms) | 10681 (308.063974ms) | 12        

--- 📡 Subscriber Summary ---
📡 Subscriber 0 (listening to entity-33) received 232 events
📡 Subscriber 1 (listening to entity-25) received 236 events
📡 Total Events Received by All Subscribers: 468

--- ❗ Detailed Error Log ---
[2025-06-20T16:26:35-04:00] Entity 'entity-14': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-21': http request for get blob failed: Get "https://green.insulalabs.io:443/db/api/v1/blob/get?key=stress-blob-397": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-40': http request for get blob failed: Get "https://green.insulalabs.io:443/db/api/v1/blob/get?key=stress-blob-53": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-6': http request for get blob failed: Get "https://green.insulalabs.io:443/db/api/v1/blob/get?key=stress-blob-838": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-34': http request for get blob failed: Get "https://green.insulalabs.io:443/db/api/v1/blob/get?key=stress-blob-677": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-43': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-36': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-17': http request for get blob failed: Get "https://green.insulalabs.io:443/db/api/v1/blob/get?key=stress-blob-182": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-28': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-47': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-33': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:26:35-04:00] Entity 'entity-24': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded

✅ Stress test on external cluster complete.
➜  build git:(dev) ✗ 

```

# With sticky client disabled, but optional (ensuring its still okay)

"time":"2025-06-20T16:41:07.38487412-04:00","level":"INFO","msg":"Attempting to connect to WebSocket for event subscription","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-41":{"url":"wss://red.insulalabs.io:443/db/api/v1/events/subscribe?topic=stress-topic"}}}}
{"time":"2025-06-20T16:41:07.385106845-04:00","level":"INFO","msg":"Attempting to connect to WebSocket for event subscription","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-31":{"url":"wss://red.insulalabs.io:443/db/api/v1/events/subscribe?topic=stress-topic"}}}}
{"time":"2025-06-20T16:41:07.581622068-04:00","level":"INFO","msg":"Successfully connected to WebSocket. Listening for events...","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-31":{"topic":"stress-topic"}}}}
{"time":"2025-06-20T16:41:07.590537922-04:00","level":"INFO","msg":"Successfully connected to WebSocket. Listening for events...","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-41":{"topic":"stress-topic"}}}}

🏁 Stress test duration completed.
{"time":"2025-06-20T16:51:07.383443075-04:00","level":"INFO","msg":"Context cancelled, closing WebSocket ping loop.","service":"fwit-t"}
{"time":"2025-06-20T16:51:07.383621041-04:00","level":"INFO","msg":"Context cancelled, closing WebSocket ping loop.","service":"fwit-t"}
{"time":"2025-06-20T16:51:07.419086005-04:00","level":"INFO","msg":"WebSocket connection closed gracefully or context cancelled.","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-41":{"error":"websocket: close 1000 (normal)"}}}}
⚠️ Subscriber 1 exited with error: websocket: close 1000 (normal)
{"time":"2025-06-20T16:51:07.421638561-04:00","level":"INFO","msg":"WebSocket connection closed gracefully or context cancelled.","service":"fwit-t","fwi-root-client":{"insi_client":{"entity-31":{"error":"websocket: close 1000 (normal)"}}}}
⚠️ Subscriber 0 exited with error: websocket: close 1000 (normal)
✅ Stress test finished.

--- 📊 Stress Test Summary ---
Entity          | Sets (avg)       | Gets (avg)       | Deletes (avg)    | Caches (avg)     | Publishes (avg)  | Bumps (avg)      | Blob Sets (avg)  | Blob Gets (avg)  | Blob Dels (avg)  | Failures  
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
entity-22       | 249   (348.475515ms) | 235   (72.804289ms) | 211   (338.107457ms) | 221   (327.783319ms) | 238   (334.700527ms) | 231   (328.333028ms) | 209   (404.108391ms) | 204   (68.878592ms) | 197   (340.409586ms) | 1         
entity-9        | 224   (335.018904ms) | 192   (73.439004ms) | 204   (326.157551ms) | 205   (336.605753ms) | 218   (334.262797ms) | 237   (332.436973ms) | 254   (412.226435ms) | 213   (72.021842ms) | 212   (344.762284ms) | 1         
entity-24       | 230   (340.780558ms) | 217   (73.966729ms) | 216   (331.838698ms) | 244   (331.754728ms) | 188   (344.223476ms) | 251   (327.597562ms) | 229   (404.882235ms) | 193   (65.905144ms) | 202   (325.978955ms) | 1         
entity-35       | 235   (330.160367ms) | 195   (75.619368ms) | 209   (339.803283ms) | 228   (319.802078ms) | 210   (340.946703ms) | 221   (333.428379ms) | 242   (412.929906ms) | 218   (76.088189ms) | 217   (328.435641ms) | 1         
entity-30       | 236   (339.536823ms) | 241   (72.102776ms) | 210   (326.796035ms) | 210   (337.10859ms) | 245   (345.105653ms) | 225   (334.387312ms) | 221   (411.46356ms) | 211   (66.4874ms) | 197   (344.055863ms) | 1         
entity-2        | 233   (330.322516ms) | 200   (74.813543ms) | 225   (333.959127ms) | 203   (330.511589ms) | 226   (323.343758ms) | 237   (332.881476ms) | 227   (417.311705ms) | 209   (69.505024ms) | 222   (332.478543ms) | 1         
entity-42       | 242   (343.647634ms) | 200   (65.910927ms) | 207   (331.619775ms) | 212   (327.273944ms) | 219   (329.282137ms) | 226   (333.152495ms) | 234   (414.857916ms) | 223   (68.932698ms) | 223   (338.123122ms) | 1         
entity-1        | 214   (332.89261ms) | 219   (72.722454ms) | 206   (335.092497ms) | 263   (327.648663ms) | 219   (335.659507ms) | 252   (341.493293ms) | 205   (414.818275ms) | 207   (66.865555ms) | 203   (337.492121ms) | 1         
entity-29       | 259   (331.584133ms) | 209   (70.297795ms) | 200   (332.685374ms) | 212   (330.360755ms) | 197   (337.92178ms) | 232   (344.280931ms) | 248   (405.357473ms) | 194   (70.199052ms) | 216   (336.964146ms) | 1         
entity-47       | 224   (338.744029ms) | 207   (62.836191ms) | 224   (340.52013ms) | 226   (334.909604ms) | 242   (319.20441ms) | 235   (324.082875ms) | 216   (408.665116ms) | 184   (77.689469ms) | 208   (346.575351ms) | 0         
entity-40       | 242   (322.355698ms) | 233   (70.71007ms) | 205   (341.923409ms) | 212   (334.599989ms) | 221   (329.966386ms) | 253   (330.215792ms) | 223   (402.883208ms) | 204   (69.082417ms) | 217   (337.377554ms) | 1         
entity-32       | 227   (333.071622ms) | 206   (66.208418ms) | 213   (331.093503ms) | 236   (334.017467ms) | 231   (332.910325ms) | 232   (336.93698ms) | 225   (393.384793ms) | 216   (70.380092ms) | 218   (331.873282ms) | 0         
entity-4        | 229   (332.145791ms) | 184   (70.159637ms) | 209   (341.07272ms) | 249   (342.288807ms) | 226   (333.050567ms) | 222   (333.53004ms) | 222   (418.40616ms) | 202   (66.95787ms) | 196   (335.545251ms) | 0         
entity-49       | 196   (333.196678ms) | 208   (69.326007ms) | 183   (337.630281ms) | 233   (329.081413ms) | 266   (329.901637ms) | 234   (335.327527ms) | 237   (405.349425ms) | 227   (68.116633ms) | 215   (339.864921ms) | 0         
entity-14       | 219   (336.726783ms) | 203   (66.27132ms) | 216   (345.375468ms) | 228   (336.231562ms) | 229   (333.618597ms) | 219   (323.983727ms) | 245   (416.871552ms) | 187   (61.7803ms) | 212   (327.445769ms) | 0         
entity-48       | 251   (340.174545ms) | 215   (74.48041ms) | 212   (336.025331ms) | 239   (335.114776ms) | 223   (336.493605ms) | 229   (330.662882ms) | 212   (401.741341ms) | 171   (70.978821ms) | 203   (333.0255ms) | 0         
entity-23       | 216   (335.27291ms) | 188   (76.425448ms) | 209   (333.83965ms) | 241   (330.458063ms) | 246   (337.685648ms) | 223   (342.961068ms) | 228   (409.725193ms) | 200   (70.04994ms) | 196   (337.810874ms) | 0         
entity-19       | 243   (335.975619ms) | 237   (71.678076ms) | 222   (335.647623ms) | 228   (340.008573ms) | 215   (338.091964ms) | 217   (340.756104ms) | 227   (397.172134ms) | 214   (73.946074ms) | 196   (337.486401ms) | 0         
entity-36       | 225   (347.357503ms) | 228   (69.457036ms) | 219   (329.043704ms) | 222   (340.099171ms) | 233   (334.624679ms) | 226   (331.970469ms) | 222   (403.515936ms) | 216   (71.919351ms) | 207   (337.661843ms) | 0         
entity-0        | 216   (336.434504ms) | 209   (64.597508ms) | 204   (331.704997ms) | 242   (337.077877ms) | 225   (326.701003ms) | 220   (317.140903ms) | 237   (395.681231ms) | 222   (67.570504ms) | 236   (347.355677ms) | 0         
entity-43       | 231   (336.927844ms) | 227   (66.621399ms) | 211   (350.639392ms) | 224   (336.759568ms) | 220   (332.602391ms) | 207   (329.085264ms) | 238   (402.257146ms) | 189   (65.689584ms) | 228   (341.437877ms) | 0         
entity-39       | 231   (339.57276ms) | 192   (73.112021ms) | 214   (336.162435ms) | 215   (333.253544ms) | 208   (344.937662ms) | 232   (344.230312ms) | 237   (394.633469ms) | 226   (66.305002ms) | 221   (331.72749ms) | 0         
entity-26       | 223   (326.635807ms) | 213   (69.246796ms) | 197   (329.695207ms) | 218   (345.690907ms) | 221   (335.930111ms) | 237   (331.464869ms) | 247   (416.111706ms) | 208   (69.066431ms) | 215   (335.536536ms) | 0         
entity-12       | 239   (329.304986ms) | 212   (73.348718ms) | 227   (334.346512ms) | 229   (336.174057ms) | 226   (330.287931ms) | 223   (341.472964ms) | 218   (394.416238ms) | 195   (72.969561ms) | 216   (328.513463ms) | 0         
entity-16       | 221   (336.402048ms) | 220   (73.798845ms) | 206   (330.047046ms) | 240   (340.470075ms) | 241   (342.801558ms) | 224   (343.074594ms) | 213   (403.858758ms) | 192   (75.230764ms) | 207   (335.127816ms) | 0         
entity-41       | 212   (325.054344ms) | 239   (69.526174ms) | 209   (325.636619ms) | 214   (331.092239ms) | 222   (338.104373ms) | 240   (338.382839ms) | 250   (406.252195ms) | 212   (72.865941ms) | 206   (349.824705ms) | 0         
entity-38       | 231   (346.68217ms) | 174   (64.4638ms) | 226   (334.933531ms) | 231   (342.720596ms) | 211   (323.461349ms) | 228   (322.1851ms) | 219   (418.82277ms) | 205   (73.441121ms) | 219   (340.137379ms) | 0         
entity-18       | 199   (350.792719ms) | 204   (73.317907ms) | 195   (335.918427ms) | 234   (330.532768ms) | 226   (326.676593ms) | 239   (332.573435ms) | 244   (406.225987ms) | 215   (65.323206ms) | 221   (339.893487ms) | 0         
entity-27       | 209   (332.55946ms) | 203   (76.993683ms) | 200   (327.8406ms) | 226   (339.130537ms) | 221   (338.535022ms) | 211   (329.826929ms) | 258   (422.038127ms) | 221   (72.069241ms) | 221   (331.943995ms) | 0         
entity-5        | 237   (334.898773ms) | 230   (70.820212ms) | 197   (322.795972ms) | 228   (345.575261ms) | 234   (327.57169ms) | 225   (327.275712ms) | 220   (423.636572ms) | 226   (64.992385ms) | 195   (350.246154ms) | 0         
entity-17       | 236   (340.235079ms) | 216   (69.762571ms) | 224   (337.613326ms) | 210   (330.009707ms) | 213   (333.10704ms) | 223   (332.984531ms) | 232   (399.566494ms) | 214   (69.309353ms) | 226   (340.087914ms) | 0         
entity-28       | 234   (337.152668ms) | 222   (83.37359ms) | 193   (317.453433ms) | 212   (330.732259ms) | 222   (346.801309ms) | 239   (339.353398ms) | 240   (404.915565ms) | 213   (67.348429ms) | 207   (331.123899ms) | 0         
entity-25       | 249   (322.514904ms) | 212   (70.797774ms) | 198   (344.759906ms) | 227   (352.049943ms) | 211   (324.58618ms) | 229   (333.278615ms) | 223   (416.907727ms) | 228   (65.6728ms) | 220   (334.389465ms) | 0         
entity-31       | 230   (334.014824ms) | 185   (67.344956ms) | 214   (329.172743ms) | 225   (343.18126ms) | 214   (337.336425ms) | 233   (342.392259ms) | 236   (399.721809ms) | 221   (64.174236ms) | 214   (338.597637ms) | 0         
entity-7        | 227   (334.44148ms) | 192   (70.015167ms) | 212   (341.667016ms) | 232   (335.721983ms) | 240   (327.490438ms) | 242   (323.049783ms) | 216   (397.867166ms) | 226   (68.649102ms) | 204   (348.035439ms) | 0         
entity-13       | 243   (336.635976ms) | 232   (70.681538ms) | 222   (326.130307ms) | 210   (348.221326ms) | 217   (325.692631ms) | 214   (328.716203ms) | 225   (406.074943ms) | 221   (79.205371ms) | 221   (340.949299ms) | 0         
entity-34       | 243   (340.137197ms) | 213   (70.578211ms) | 237   (340.826837ms) | 233   (322.082408ms) | 243   (339.316652ms) | 245   (329.309732ms) | 191   (401.958229ms) | 188   (67.661662ms) | 188   (334.54518ms) | 0         
entity-21       | 229   (334.076943ms) | 192   (70.746911ms) | 212   (332.667918ms) | 227   (331.564467ms) | 240   (336.026449ms) | 212   (351.575888ms) | 236   (421.205086ms) | 212   (67.668957ms) | 184   (344.58624ms) | 0         
entity-8        | 217   (342.737449ms) | 215   (69.56747ms) | 196   (325.846747ms) | 245   (331.795454ms) | 226   (335.663221ms) | 235   (337.306861ms) | 222   (407.504124ms) | 226   (73.786277ms) | 187   (339.247492ms) | 0         
entity-3        | 233   (340.085271ms) | 212   (72.134334ms) | 232   (330.797304ms) | 236   (330.376498ms) | 226   (329.702551ms) | 234   (338.637114ms) | 205   (415.133668ms) | 195   (69.170253ms) | 201   (338.661777ms) | 0         
entity-10       | 218   (332.514509ms) | 186   (76.349799ms) | 203   (337.310552ms) | 210   (336.732374ms) | 221   (328.152432ms) | 245   (333.831933ms) | 240   (411.033869ms) | 216   (69.217352ms) | 227   (335.445824ms) | 0         
entity-6        | 196   (340.044623ms) | 176   (61.936901ms) | 196   (332.36701ms) | 238   (335.378214ms) | 232   (340.387461ms) | 240   (337.652758ms) | 226   (406.692764ms) | 223   (74.473415ms) | 224   (346.920338ms) | 0         
entity-11       | 240   (336.931593ms) | 208   (68.684433ms) | 239   (327.380395ms) | 209   (331.879782ms) | 212   (339.37579ms) | 228   (340.409509ms) | 221   (409.032497ms) | 206   (69.817389ms) | 198   (327.162378ms) | 0         
entity-44       | 248   (334.325978ms) | 224   (65.214753ms) | 222   (336.2805ms) | 207   (336.398909ms) | 211   (329.195204ms) | 204   (332.282624ms) | 244   (401.619745ms) | 228   (71.989994ms) | 223   (342.729907ms) | 0         
entity-20       | 232   (344.892231ms) | 200   (68.141714ms) | 213   (329.163912ms) | 242   (327.529906ms) | 203   (343.608908ms) | 205   (327.794855ms) | 233   (398.448839ms) | 218   (76.305326ms) | 231   (349.261381ms) | 0         
entity-45       | 248   (329.078276ms) | 212   (78.978592ms) | 207   (337.781494ms) | 236   (330.232037ms) | 211   (335.02314ms) | 217   (341.905509ms) | 221   (407.105822ms) | 208   (68.408862ms) | 216   (342.682577ms) | 0         
entity-46       | 199   (325.178145ms) | 216   (72.272678ms) | 196   (335.404181ms) | 264   (339.529773ms) | 229   (336.950248ms) | 244   (341.595895ms) | 218   (400.973224ms) | 198   (75.803405ms) | 210   (338.430961ms) | 0         
entity-15       | 244   (335.73891ms) | 234   (71.025278ms) | 218   (339.600196ms) | 202   (345.581011ms) | 217   (340.821955ms) | 229   (325.347749ms) | 233   (398.268361ms) | 204   (65.745941ms) | 220   (330.521709ms) | 0         
entity-37       | 232   (344.307987ms) | 223   (78.442746ms) | 214   (329.91863ms) | 240   (336.767718ms) | 228   (336.867015ms) | 208   (347.397871ms) | 223   (402.220859ms) | 210   (68.133082ms) | 208   (327.292583ms) | 0         
entity-33       | 229   (331.793484ms) | 224   (69.348689ms) | 224   (337.688038ms) | 219   (326.011338ms) | 242   (343.676984ms) | 229   (334.431404ms) | 226   (406.203751ms) | 213   (76.140295ms) | 187   (338.048559ms) | 0         
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
TOTALS          | 11470 (335.982301ms) | 10534 (71.059313ms) | 10558 (334.008092ms) | 11337 (334.885727ms) | 11205 (334.516537ms) | 11443 (334.275905ms) | 11418 (407.092013ms) | 10472 (70.00325ms) | 10538 (337.889125ms) | 10        

--- 📡 Subscriber Summary ---
📡 Subscriber 0 (listening to entity-31) received 214 events
📡 Subscriber 1 (listening to entity-41) received 222 events
📡 Total Events Received by All Subscribers: 436

--- ❗ Detailed Error Log ---
[2025-06-20T16:51:07-04:00] Entity 'entity-29': http request for blob upload failed: Post "https://red.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-22': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-40': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-42': http request for get blob failed: Get "https://red.insulalabs.io:443/db/api/v1/blob/get?key=stress-blob-552": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-24': http request for get blob failed: Get "https://red.insulalabs.io:443/db/api/v1/blob/get?key=stress-blob-855": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-35': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-30': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-2': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-1': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded
[2025-06-20T16:51:07-04:00] Entity 'entity-9': http request for blob upload failed: Post "https://green.insulalabs.io:443/db/api/v1/blob/set": context deadline exceeded

✅ Stress test on external cluster complete.
➜  build git:(sticky-client) ✗ 