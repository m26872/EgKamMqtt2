/*
Sketch to read the Kamstrup Omnipower 3ph electrical meter with HAN/P1-module with binary HDLC-frame protocol (IEC62056-7-5) at 2400 baud.
/m26872 2022-10-16

Hardware: Arduino Mega with Ethernet module and P1/RJ12-interface 
Normal inverting P1/RJ12-interface, e.g. similar to "https://create.arduino.cc/projecthub/voske65/p1-energy-hub-f88865"
but with Kamstrup special +5V supply FROM the Arduino Mega TO the meter HAN/P1-module.

*/

#include <SPI.h>
#include <Ethernet.h>
#include <PubSubClient.h>

#define HOSTNAME "kamgw"
#define MQTT_RECONNECT_ATTEMPT_TIME 3000
//#define MQTT_MAX_RECONNECT_TRIES 10
#define ROOT_TOPIC "kamgw-pub"

#define MAX_RX_BUFFER 350
#define KAMBAUD 2400
#define READTIMEOUT 10

#define SERIAL1_RX_PIN 19
#define SERIAL1_TX_PIN 18  //OBS! Tx1/Rx1 wrong label on some arduino-mega-clones
#define RTS_ENABLE_PIN 33

#define END_RECEIVED -7
#define TIMEOUT_ERROR -5
#define READ 1
#define SEND 2
#define IDLE 3
#define DUMMYREAD 4

EthernetClient ethClient;
PubSubClient client(ethClient);
byte mac[]    = { 0xb6, 0x5d, 0x49, 0x84, 0xc6, 0xF9 };
IPAddress server(192, 168, 1, 29);
long mqttReconnectTimer;

const byte OBISin[4] = {0x09, 0x06, 0x01, 0x01};  // 09 06 and OBIS byte "A" and "B" byte "C" and "D" as follows:
//const byte OBIStime[4] = {0x09, 0x06, 0x00, 0x01};  // 09 06 and OBIS byte "A" and "B"
const byte OBIS_Id[2] = {0x00, 0x00}; // OBIS meter ID
const byte OBIS_Model[2] = {0x60, 0x01}; // OBIS meter model
const byte OBIS_Pimp[2] = {0x01, 0x07};  // Ex. Pimp 1365kW:   09 06 01 01   01 07   00 FF 06   00 00 05 55
const byte OBIS_Pexp[2] = {0x02, 0x07};
const byte OBIS_Qimp[2] = {0x03, 0x07};
const byte OBIS_Qexp[2] = {0x04, 0x07};
const byte OBIS_I1[2] = {0x1F, 0x07};  // Ex. I1=169 (1.68A): 09 06 01 01  1F 07   00 FF 06   00 00 00 A9 (169)
const byte OBIS_I2[2] = {0x33, 0x07};
const byte OBIS_I3[2] = {0x47, 0x07};
const byte OBIS_U1[2] = {0x20, 0x07};  // Ex. U1= 232V: 09 06 01 01   20 07   00 FF 12   00 E8 (232)
const byte OBIS_U2[2] = {0x34, 0x07};
const byte OBIS_U3[2] = {0x48, 0x07};
const byte OBIS_Phimp[2] = {0x01, 0x08};  // Ex. Ph_imp 876.219kWh:   09 06 01 01    01 08    00 FF 06    00 0D 5E BB (876219)
const byte OBIS_Phexp[2] = {0x02, 0x08};
const byte OBIS_Qhimp[2] = {0x03, 0x08};
const byte OBIS_Qhexp[2] = {0x04, 0x08};
//const byte OBIS_timestamp[] = {0x01 0x00};
unsigned long Pimp, Pexp, Qimp, Qexp, Phimp, Phexp, Qhimp, Qhexp = 0; // 4 byte, 32 bit 
unsigned int I1, I2, I3, U1, U2, U3, TsY, TsM, TsD, Tsh, Tsm = 0;  // 2 byte, 16 bit
unsigned long Pimp_last, Pexp_last, Qimp_last, Qexp_last, Phimp_last, Phexp_last, Qhimp_last, Qhexp_last = 0;
unsigned int I1_last, I2_last, I3_last, U1_last, U2_last, U3_last = 0;

byte recvmsg[MAX_RX_BUFFER];
int lastByteIdx = -1;
void printBytes();
int state = 0;
byte r;
long lastRead;
long lastUpdate;
long now;

void setup() {

  Serial.begin(115200);
  Serial.println(F("Serial started."));
  Serial1.begin(KAMBAUD, SERIAL_8N1);
  Serial.print(F("Serial_1 started with "));Serial.print(KAMBAUD);Serial.println(F(" baud."));
  pinMode(LED_BUILTIN, OUTPUT);
  pinMode(SERIAL1_RX_PIN, INPUT);
  digitalWrite(SERIAL1_RX_PIN, HIGH);
  digitalWrite(LED_BUILTIN, HIGH);
  delay(200);
  
  Serial.println("Initializing Ethernet connection and wait for DHCP...");
  if (Ethernet.begin(mac) == 0) {
    Serial.println("Failed to configure Ethernet using DHCP");
  }
  printIPAddress();
  Serial.println("Setting MQTT server...");
  delay(1500);
  client.setServer(server, 1883);
  delay(200);

  while(Serial.available()){Serial.read();} // clear Serial buffer
  while(Serial1.available()){Serial1.read();} // clear Serial1 buffer

  state = IDLE;
  lastRead = millis();
  lastUpdate = millis();
  mqttReconnectTimer = millis();
  Serial.println("Setup done! Let's GO!");
}
  
  
void loop () {
	
	if (state == IDLE) {
		if (Serial1.available()) {
			state = READ; // Serial.println("state= READ");
			r = Serial1.read();
			lastRead = millis();
			if (r == 0x7E) { // start received
				lastByteIdx++;
				recvmsg[lastByteIdx] = r;
			} else { // something else came - show and drop
				Serial.print("Other than start-char 7E received: "); Serial.println(r,HEX);
				state = DUMMYREAD; // Serial.println("state= DUMMYREAD");
			}
		} else {  // Do all the other important stuff here 
			reconnectMQTT();
			if ( client.connected() ) {
			  Ethernet.maintain();
			  client.loop();
			}
		}
	} else if (state == READ) {
		if (Serial1.available()) {
			r = Serial1.read();
			lastByteIdx++;
			recvmsg[lastByteIdx] = r;
			lastRead = millis();
		} else if (millis() - lastRead > READTIMEOUT) {
//			if (recvmsg[lastByteIdx]==0x7E) Serial.println("Check OK! End flag received after READTIMEOUT!");
			state = SEND;
		}
	} else if (state == DUMMYREAD) {
		 if (Serial1.available()) {
			 r = Serial1.read();
			 lastRead = millis();
			 if (r == 0x7E) { // end received
				 state = IDLE;  // Serial.println("state= IDLE");
			 }
		 }
	} else if (state == SEND) {
//		Serial.println(F("Sending... : "));
//		Serial.print(lastByteIdx+1); Serial.print(" ");  // Show message length
/*		if ((lastByteIdx+1 > 230) || (lastByteIdx+1 < 220) ){  // If unusual size show message
			Serial.println();
			Serial.println(F("Read >230 or <220 bytes: "));
			printBytes();
		}
*/
		decode();
//		printResult();
		if (client.connected()) {
			publishMeterVal();
		} else {
			Serial.println("Client not connected - Nothing published!");
		}
		lastByteIdx = -1;   // Reset Idx
		state = IDLE;  // Serial.println("state= IDLE");
	}
	
}

void reconnectMQTT() { //Reconnect to MQTT server and subscribe to in and out topics. NB no MQTT_MAX_RECONNECT_TRIES is used, i.e. search forever until conencted.
  while (!client.connected()) {
    Ethernet.maintain();
    if ( millis() - mqttReconnectTimer > MQTT_RECONNECT_ATTEMPT_TIME ) {
      Serial.print("Attempting MQTT connection...");
      if (client.connect("arduinoKamstrupClient")) {
        Serial.println("kmagw reconnected!");
        client.publish("kamgw-pub/status","kamgw reconnected!"); // Once connected, publish an announcement... 
 //       client.subscribe("kamgw-in/command"); // ... and resubscribe       
      } else {
        Serial.print("failed, rc=");
        Serial.print(client.state());
        Serial.print(" try again in "); Serial.print(MQTT_RECONNECT_ATTEMPT_TIME); Serial.println(" seconds");
        mqttReconnectTimer = millis();
      }
    }
  }
}	

void printBytes() {
//	Serial.println(lastByteIdx);
    if (lastByteIdx > -1) {
      for (int i = 0; i <= lastByteIdx; i++) { // ska det vara lastByteIdx+1 ?
         Serial.print(recvmsg[i], HEX);
		 Serial.print(" ");
		 if (i+3 < lastByteIdx && recvmsg[i]==0x09 && recvmsg[i+1]==0x06 && recvmsg[i+2]==0x01 && recvmsg[i+3]==0x01) {
			 Serial.println();
		 }
      }
    } else {
      Serial.println(F("Print Error: 0 bytes received!"));
    }
	Serial.println();
}

void decode() {
	int i = 1; // Skip the start flag
	while ( i+3 < lastByteIdx) {  // 
		while ( !(recvmsg[i]==OBISin[0] && recvmsg[i+1]==OBISin[1] && recvmsg[i+2]==OBISin[2] && recvmsg[i+3]==OBISin[3] ) && !(i+3>MAX_RX_BUFFER-1) && (i+3<lastByteIdx) ) {
			i++;
		}
		if (i+3 > MAX_RX_BUFFER-1) {
			Serial.println("Decode error: i+3 > MAX_RX_BUFFER-1");
		} else {
			 i+=4;
			 if (recvmsg[i]==OBIS_Id[0] && recvmsg[i+1]==OBIS_Id[1]) {
		//        Serial.println("OBIS_Id found!");
				i+=6;
				// Build string here (16 digit ascii) <5706567343092014>
				i+=16;
			 } else if (recvmsg[i]==OBIS_Model[0] && recvmsg[i+1]==OBIS_Model[1]) {
		//        Serial.println("OBIS_Model found!");
				i+=5;
				// Build string here (18 bytes ?)
				i+=18;
			 } else if (recvmsg[i]==OBIS_Pimp[0] && recvmsg[i+1]==OBIS_Pimp[1]) {
//				Serial.println("OBIS_Pimp found!");
				i+=5;
				Pimp = (unsigned long)recvmsg[i] << 24;
				Pimp += (unsigned long)recvmsg[i+1] << 16;
				Pimp += (unsigned long)recvmsg[i+2] << 8;
				Pimp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_Pexp[0] && recvmsg[i+1]==OBIS_Pexp[1]) {
//				Serial.println("OBIS_Pexp found!");
				i+=5;
				Pexp = (unsigned long)recvmsg[i] << 24;
				Pexp += (unsigned long)recvmsg[i+1] << 16;
				Pexp += (unsigned long)recvmsg[i+2] << 8;
				Pexp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_Qimp[0] && recvmsg[i+1]==OBIS_Qimp[1]) {
//				Serial.println("OBIS_Qimp found!");
				i+=5;
				Qimp = (unsigned long)recvmsg[i] << 24;
				Qimp += (unsigned long)recvmsg[i+1] << 16;
				Qimp += (unsigned long)recvmsg[i+2] << 8;
				Qimp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_Qexp[0] && recvmsg[i+1]==OBIS_Qexp[1]) {
//				Serial.println("OBIS_Qexp found!");
				i+=5;
				Qexp = (unsigned long)recvmsg[i] << 24;
				Qexp += (unsigned long)recvmsg[i+1] << 16;
				Qexp += (unsigned long)recvmsg[i+2] << 8;
				Qexp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_I1[0] && recvmsg[i+1]==OBIS_I1[1]) {
//				Serial.println("OBIS_I1 found!");
				i+=5;
				I1 = (unsigned long)recvmsg[i] << 24;
				I1 += (unsigned long)recvmsg[i+1] << 16;
				I1 += (unsigned long)recvmsg[i+2] << 8;
				I1 += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_I2[0] && recvmsg[i+1]==OBIS_I2[1]) {
//				Serial.println("OBIS_I2 found!");
				i+=5;
				I2 = (unsigned long)recvmsg[i] << 24;
				I2 += (unsigned long)recvmsg[i+1] << 16;
				I2 += (unsigned long)recvmsg[i+2] << 8;
				I2 += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_I3[0] && recvmsg[i+1]==OBIS_I3[1]) {
//				Serial.println("OBIS_I3 found!");
				i+=5;
				I3 = (unsigned long)recvmsg[i] << 24;
				I3 += (unsigned long)recvmsg[i+1] << 16;
				I3 += (unsigned long)recvmsg[i+2] << 8;
				I3 += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_U1[0] && recvmsg[i+1]==OBIS_U1[1]) {
//				Serial.println("OBIS_U1 found!");
				i+=5;
				U1 = (unsigned int)recvmsg[i] << 8;
				U1 += (unsigned int)recvmsg[i+1];
				i+=2;
			} else if (recvmsg[i]==OBIS_U2[0] && recvmsg[i+1]==OBIS_U2[1]) {
//				Serial.println("OBIS_U2 found!");
				i+=5;
				U2 = (unsigned int)recvmsg[i] << 8;
				U2 += (unsigned int)recvmsg[i+1];
				i+=2;
			} else if (recvmsg[i]==OBIS_U3[0] && recvmsg[i+1]==OBIS_U3[1]) {
//				Serial.println("OBIS_U3 found!");
				i+=5;
				U3 = (unsigned int)recvmsg[i] << 8;
				U3 += (unsigned int)recvmsg[i+1];
				i+=2;
			} else if (recvmsg[i]==OBIS_Phimp[0] && recvmsg[i+1]==OBIS_Phimp[1]) {
//				Serial.println("OBIS_Phimp found!");
				i+=5;
				Phimp = (unsigned long)recvmsg[i] << 24;
				Phimp += (unsigned long)recvmsg[i+1] << 16;
				Phimp += (unsigned long)recvmsg[i+2] << 8;
				Phimp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_Phexp[0] && recvmsg[i+1]==OBIS_Phexp[1]) {
//				Serial.println("OBIS_Phexp found!");
				i+=5;
				Phexp = (unsigned long)recvmsg[i] << 24;
				Phexp += (unsigned long)recvmsg[i+1] << 16;
				Phexp += (unsigned long)recvmsg[i+2] << 8;
				Phexp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_Qhimp[0] && recvmsg[i+1]==OBIS_Qhimp[1]) {
//				Serial.println("OBIS_Qhimp found!");
				i+=5;
				Qhimp = (unsigned long)recvmsg[i] << 24;
				Qhimp += (unsigned long)recvmsg[i+1] << 16;
				Qhimp += (unsigned long)recvmsg[i+2] << 8;
				Qhimp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else if (recvmsg[i]==OBIS_Qhexp[0] && recvmsg[i+1]==OBIS_Qhexp[1]) {
//				Serial.println("OBIS_Qhexp found!");
				i+=5;
				Qhexp = (unsigned long)recvmsg[i] << 24;
				Qhexp += (unsigned long)recvmsg[i+1] << 16;
				Qhexp += (unsigned long)recvmsg[i+2] << 8;
				Qhexp += (unsigned long)recvmsg[i+3];
				i+=4;
			} else {
				Serial.print("Decode error: Unknown OBIS at index "); Serial.print(i); Serial.print(" : "); Serial.print(recvmsg[i],HEX); Serial.print(" "); Serial.println(recvmsg[i+1],HEX);
				printBytes();
				i+=2;
			}
		}
	}
}

void publishMeterVal() {
//unsigned long Pimp, Pexp, Qimp, Qexp, Phimp, Phexp, Qhimp, Qhexp = 0; // 4 byte, 32 bit 
//unsigned int I1, I2, I3, U1, U2, U3, TsY, TsM, TsD, Tsh, Tsm = 0;  // 2 byte, 16 bit
  if (Pimp != Pimp_last) {
    send_metric("Pimp", Pimp);
    Pimp_last = Pimp; 
  }
  if (Pexp != Pexp_last) {
    send_metric("Pexp", Pexp);
    Pexp_last = Pexp; 
  }
  if (Qimp != Qimp_last) {
    send_metric("Qimp", Qimp);
    Qimp_last = Qimp; 
  }
  if (Qexp != Qexp_last) {
    send_metric("Qexp", Qexp);
    Qexp_last = Qexp; 
  }
  if (Phimp != Phimp_last) {
    send_metric("Phimp", Phimp);
    Phimp_last = Phimp; 
  }
  if (Phexp != Phexp_last) {
    send_metric("Phexp", Phexp);
    Phexp_last = Phexp; 
  }
  if (Qhimp != Qhimp_last) {
    send_metric("Qhimp", Qhimp);
    Qhimp_last = Qhimp; 
  }
  if (Qhexp != Qhexp_last) {
    send_metric("Qhexp", Qhexp);
    Qhexp_last = Qhexp; 
  }
  if (I1 != I1_last) {
    send_metric("I1", (unsigned long)I1);
    I1_last = I1; 
  }
  if (I2 != I2_last) {
    send_metric("I2", (unsigned long)I2);
    I2_last = I2; 
  }  
  if (I3 != I3_last) {
    send_metric("I3", (unsigned long)I3);
    I3_last = I3; 
  }
  if (U1 != U1_last) {
    send_metric("U1", (unsigned long)U1);
    I1_last = I1; 
  }
  if (U2 != U2_last) {
    send_metric("U2", (unsigned long)U2);
    U2_last = U2; 
  }  
  if (U3 != U3_last) {
    send_metric("U3", (unsigned long)U3);
    U3_last = U3; 
  }
}  

void send_metric(String name, long metric) {
//    Serial.print(F("Sending metric to broker: ")); Serial.print(name); Serial.print(F("=")); Serial.println(metric);
    char output[10];
    ltoa(metric, output, sizeof(output));
    String topic = String(ROOT_TOPIC) + "/" + name;
    send_mqtt_message(topic.c_str(), output);
}

void send_mqtt_message(const char *topic, char *payload) { // * Send a message to a broker topic
//    Serial.print(F("MQTT outgoing on topic:")); Serial.print(topic); Serial.print(" with payload: "); Serial.println(payload);
    bool result = client.publish(topic, payload, false);
    if (!result) {
        Serial.print(F("MQTT publish to topic ")); Serial.print(topic); Serial.println(" failed.");
    }
}

void printResult() {
	Serial.print("Pimp "); Serial.print(Pimp); Serial.print("   "); Serial.print("Pexp "); Serial.println(Pexp);
	Serial.print("Qimp "); Serial.print(Qimp); Serial.print("   "); Serial.print("Qexp "); Serial.println(Qexp);
	Serial.print("I1 "); Serial.print(I1); Serial.print("   "); Serial.print("I2 "); Serial.print(I2); Serial.print("   ");Serial.print("I3 "); Serial.println(I3);
	Serial.print("U1 "); Serial.print(U1); Serial.print("   "); Serial.print("U2 "); Serial.print(U2); Serial.print("   ");Serial.print("U3 "); Serial.println(U3);
	Serial.print("Phimp "); Serial.print(Phimp); Serial.print("   "); Serial.print("Phexp "); Serial.println(Phexp);
	Serial.print("Qhimp "); Serial.print(Qhimp); Serial.print("   "); Serial.print("Qhexp "); Serial.println(Qhexp);
}

void printIPAddress() {
  Serial.print("My IP address: ");
  for (byte thisByte = 0; thisByte < 4; thisByte++) {
    // print the value of each byte of the IP address:
    Serial.print(Ethernet.localIP()[thisByte], DEC);
    Serial.print(".");
  }
  Serial.println();
}
