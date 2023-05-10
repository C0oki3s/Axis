package com.wheelseye.cyborg.client.axisbank.impl;

import com.wheelseye.cyborg.client.axisbank.AxisBankClient;
import com.wheelseye.cyborg.client.axisbank.dto.*;
import com.wheelseye.cyborg.constant.Constants;
import com.wheelseye.cyborg.exception.ApiException;
import com.wheelseye.cyborg.exception.BankClientException;
import com.wheelseye.cyborg.service.SequenceGenerator;
import com.wheelseye.cyborg.util.CustomErrorMessages;
import com.wheelseye.cyborg.util.EncryptionDecryptionUtil;
import com.wheelseye.cyborg.util.TransformUtil;
import io.sentry.Sentry;
import lombok.extern.log4j.Log4j2;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

@Service
@Log4j2
public class AxisBankClientImpl implements AxisBankClient {

    @Value("${axis.bank.url}")
    private String bankUrl;

    @Value("${axis.bank.client-id}")
    private String CLIENT_ID;

    @Value("${axis.bank.client-secret}")
    private String CLIENT_SECRET;

    @Value("${axis.bank.channel-id}")
    private String CHANNEL_ID;

    @Value("${axis.bank.checksum-key}")
    private String CHECKSUM_KEY;

    @Value("${axis.bank.encryption-decryption-key}")
    private String ENCRYPTION_DECRYPTION_KEY;

    @Value("${axis.bank.key-store-password}")
    private String KEY_STORE_PASSWORD;

    @Value("${axis.bank.trust-store-password}")
    private String TRUST_STORE_PASSWORD;

    @Value("${axis.bank.key-password}")
    private String KEY_PASSWORD;

    @Value("${axis.bank.pos-id}")
    private String POS_NUMBER;

    @Autowired
    private SequenceGenerator seqGenerator;

    private RestTemplate axisClient;

    // separate client with high time out to ensure no/less timeouts in axis
    // onboarding because
    // axis npci status api doesn't provide tag id which makes it difficult to sync
    // time out cases with confidence
    private RestTemplate axisOnboardingClient;

    private RestTemplate axisBalanceSyncClient;

    @PostConstruct
    public void setClients() {
        PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager(
                getSslConnectionFactory());
        poolingHttpClientConnectionManager.setMaxTotal(120);
        poolingHttpClientConnectionManager.setDefaultMaxPerRoute(5);
        CloseableHttpClient httpClient = getHttpClient(poolingHttpClientConnectionManager, 16000, 2500, 20000);
        axisClient = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient));

        CloseableHttpClient httpClientBalanceSync = getHttpClient(poolingHttpClientConnectionManager, 2000, 2000, 2000);
        axisBalanceSyncClient = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClientBalanceSync));

        CloseableHttpClient httpOnboardingClient = getHttpClient(poolingHttpClientConnectionManager, 30000, 2500,
                20000);
        axisOnboardingClient = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpOnboardingClient));
    }

    private Registry<ConnectionSocketFactory> getSslConnectionFactory() {
        Registry<ConnectionSocketFactory> socketFactoryRegistry;
        try {
            String env = System.getProperty("env");
            // there are same trust stores for scheduler and app
            env = env.replace("fastag-scheduler-", "");
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(
                    new SSLContextBuilder()
                            .loadTrustMaterial(ResourceUtils.getFile("classpath:ssl/wheelseye-axis-ts-" + env + ".jks"),
                                    TRUST_STORE_PASSWORD.toCharArray())
                            .loadKeyMaterial(ResourceUtils.getFile("classpath:ssl/wheelseye-axis-ks-" + env + ".jks"),
                                    KEY_STORE_PASSWORD.toCharArray(), KEY_PASSWORD.toCharArray())
                            .build(),
                    NoopHostnameVerifier.INSTANCE);
            socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", sslConnectionSocketFactory).build();
        } catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | CertificateException
                | IOException | KeyManagementException e) {
            LOG.error("Axis SSLConnectionSocketFactory initialization failure {}", e.fillInStackTrace(), e);
            throw new ApiException("Axis SSLConnectionSocketFactory could not be initialized");
        }
        return socketFactoryRegistry;
    }

    private CloseableHttpClient getHttpClient(PoolingHttpClientConnectionManager poolingHttpClientConnectionManager,
            int socketTimeOut, int connectTimeout, int connectionRequestTimeout) {
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(connectionRequestTimeout)
                .setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeOut).build();
        return HttpClientBuilder.create()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(requestConfig).build();
    }

    private HttpHeaders createHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-IBM-Client-Id", CLIENT_ID);
        headers.set("X-IBM-Client-Secret", CLIENT_SECRET);
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    @Override
    public AxisNPCITagStatusResponseDTO npciTagStatus(AxisNPCITagStatusRequestDTO npciTagStatusRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("NPCISTATUS" + seqGenerator.nextId(), "1239994");
        npciTagStatusRequestDTO.createCheckSum(CHECKSUM_KEY, subHeaderDTO);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.NPCI_TAG_STATUS);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("VehicleStatusRequest", encrypt(npciTagStatusRequestDTO), subHeaderDTO),
                createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("npciTagStatus request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(npciTagStatusRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisNPCITagStatusResponseDTO decryptedResponse = decrypt(response.getBody(), "VehicleStatusResponse",
                    AxisNPCITagStatusResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("npciTagStatus response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("npciTagStatus response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisWalletVehicleCreationResponseDTO walletVehicleCreation(
            AxisWalletVehicleCreationRequestDTO walletVehicleCreationRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("WALLETCREATION" + seqGenerator.nextId(), "WALLET");
        walletVehicleCreationRequestDTO.createCheckSum(CHECKSUM_KEY, subHeaderDTO);
        walletVehicleCreationRequestDTO.setChannelId(subHeaderDTO.getChannelId());
        walletVehicleCreationRequestDTO.setPosNumber(POS_NUMBER);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.WALLET_VEHICLE_CREATION);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("WalletRequest", encrypt(walletVehicleCreationRequestDTO), subHeaderDTO),
                createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            if (walletVehicleCreationRequestDTO.getOperationId().equals("1")) {
                LOG.info("WalletCreation request :: {} sent on Axis, sub header {}",
                        TransformUtil.toJson(walletVehicleCreationRequestDTO), subHeaderDTO);
            } else {
                LOG.info("VehicleCreation request :: {} sent on Axis, sub header {}",
                        TransformUtil.toJson(walletVehicleCreationRequestDTO), subHeaderDTO);
            }
            ResponseEntity<Map<String, Map<String, Object>>> response = axisOnboardingClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisWalletVehicleCreationResponseDTO decryptedResponse = decrypt(response.getBody(), "WalletResponse",
                    AxisWalletVehicleCreationResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            if (walletVehicleCreationRequestDTO.getOperationId().equals("1")) {
                LOG.info("WalletCreation response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            } else {
                LOG.info("VehicleCreation response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            }
            return decryptedResponse;
        } catch (ResourceAccessException e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            if (e.contains(ConnectionPoolTimeoutException.class)) {
                LOG.error("axis wallet/vehicle creation connection pool timeout in {}, exception {}",
                        responseTimeInMillis, e.fillInStackTrace());
            } else if (e.contains(SSLException.class) || e.contains(SocketTimeoutException.class)) {
                LOG.error("axis wallet/vehicle creation read timeout in {}, exception {}", responseTimeInMillis,
                        e.fillInStackTrace());
            } else {
                LOG.error("axis wallet/vehicle creation unknown resource access exception {} in {}",
                        e.fillInStackTrace(), responseTimeInMillis);
            }
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(Constants.AXISBank.Exceptions.API_TIMEOUT_EXCEPTION);
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("WalletVehicleCreation response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisCustomerDedupeResponseDTO verifyCustomerDuplicacy(
            AxisCustomerDedupeRequestDTO customerDedupeRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("CUSTOMERDEDUPE" + seqGenerator.nextId(), "Dedupe");
        customerDedupeRequestDTO.createCheckSum(CHECKSUM_KEY, subHeaderDTO);
        customerDedupeRequestDTO.setChannelId(subHeaderDTO.getChannelId());
        customerDedupeRequestDTO.setPosNumber(POS_NUMBER);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.CUSTOMER_DEDUPE);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("DedupeRequest", encrypt(customerDedupeRequestDTO), subHeaderDTO), createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("CustomerDedupe request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(customerDedupeRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisCustomerDedupeResponseDTO decryptedResponse = decrypt(response.getBody(), "DedupeResponse",
                    AxisCustomerDedupeResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("CustomerDedupe response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("CustomerDedupe response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisGenerateOTPResponseDTO generateOTPForUserOnBoarding(AxisGenerateOTPRequestDTO generateOTPRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("WEOTP" + seqGenerator.nextId(), "GENERATE-OTP");
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.GENERATE_OTP);

        Map<String, Object> encryptedReq = getRequestBody("NHAICCHGenerateOTPRequest", encrypt(generateOTPRequestDTO),
                subHeaderDTO);
        HttpEntity<Object> entity = new HttpEntity<>(encryptedReq, createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("generateOTPForUserOnBoarding request :: {} sent on Axis, encryptedReq {}",
                    TransformUtil.toJson(generateOTPRequestDTO), TransformUtil.toJson(encryptedReq));
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisGenerateOTPResponseDTO decryptedResponse = decrypt(response.getBody(), "NHAICCHGenerateOTPResponse",
                    AxisGenerateOTPResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("generateOTPForUserOnBoarding response {} received after {} ms", decryptedResponse,
                    responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("generateOTPForUserOnBoarding response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisVerifyOTPResponseDTO verifyOTPForUserOnBoarding(AxisVerifyOTPRequestDTO verifyOTPRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("VERIFYOTP" + seqGenerator.nextId(), "VALIDATE-OTP");
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.VERIFY_OTP);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("NHAICCHValidateOTPRequest", encrypt(verifyOTPRequestDTO), subHeaderDTO),
                createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("verifyOTPForUserOnBoarding request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(verifyOTPRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisVerifyOTPResponseDTO decryptedResponse = decrypt(response.getBody(), "NHAICCHValidateOTPResponse",
                    AxisVerifyOTPResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("verifyOTPForUserOnBoarding response {} received after {} ms", decryptedResponse,
                    responseTimeInMillis);
            return decryptedResponse;
        } catch (HttpClientErrorException e) {
            if ((e.getMessage() != null && e.getMessage().contains("Invalid OTP"))
                    || e.contains(HttpClientErrorException.Forbidden.class)) {
                LOG.warn("verifyOTPForUserOnBoarding response mismatch exception {} , {}", e.getMessage(),
                        e.fillInStackTrace());
                return null;
            }
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            LOG.error("verifyOTPForUserOnBoarding response HttpClientErrorException exception {}",
                    e.fillInStackTrace());
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("verifyOTPForUserOnBoarding response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisRechargeResponseDTO validateRecharge(AxisRechargeRequestDTO rechargeRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("VALIDATERECHARGE" + seqGenerator.nextId(), "RECHARGE");
        rechargeRequestDTO.setMode("V");
        rechargeRequestDTO.setMerchantId(CHANNEL_ID);
        rechargeRequestDTO.createCheckSum(CHECKSUM_KEY);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.RECHARGE);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("RechargeRequest", encrypt(rechargeRequestDTO), subHeaderDTO), createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("validateRecharge request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(rechargeRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisRechargeResponseDTO decryptedResponse = decrypt(response.getBody(), "RechargeResponse",
                    AxisRechargeResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("validateRecharge response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("validateRecharge response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisRechargeResponseDTO rechargeWallet(AxisRechargeRequestDTO rechargeRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("RECHARGE" + seqGenerator.nextId(), "RECHARGE");
        rechargeRequestDTO.setMode("R");
        rechargeRequestDTO.setMerchantId(CHANNEL_ID);
        rechargeRequestDTO.createCheckSum(CHECKSUM_KEY);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.RECHARGE);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("RechargeRequest", encrypt(rechargeRequestDTO), subHeaderDTO), createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("rechargeWallet request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(rechargeRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisRechargeResponseDTO decryptedResponse = decrypt(response.getBody(), "RechargeResponse",
                    AxisRechargeResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("rechargeWallet response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("rechargeWallet response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisSyncWalletBalanceResponseDTO getWalletBalance(
            AxisSyncWalletBalanceRequestDTO syncWalletBalanceRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("WALLETBALANCE" + seqGenerator.nextId(), "1239994");
        syncWalletBalanceRequestDTO.createCheckSum(CHECKSUM_KEY, subHeaderDTO);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.GET_WALLET_BALANCE);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("WalletBalanceRequest", encrypt(syncWalletBalanceRequestDTO), subHeaderDTO),
                createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("getWalletBalance request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(syncWalletBalanceRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisBalanceSyncClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisSyncWalletBalanceResponseDTO decryptedResponse = decrypt(response.getBody(), "WalletBalanceResponse",
                    AxisSyncWalletBalanceResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("getWalletBalance response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("getWalletBalance response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisRechargeStatusEnquiryResponseDTO rechargeStatusEnquiry(
            AxisRechargeStatusEnquiryRequestDTO rechargeStatusEnquiryRequestDTO) {
        rechargeStatusEnquiryRequestDTO.setField1(CHANNEL_ID);
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("RECHARGEENQUIRY" + seqGenerator.nextId(),
                "RECHARGE-STATUS-ENQUIRY");
        rechargeStatusEnquiryRequestDTO.createCheckSum(CHECKSUM_KEY);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.RECHARGE_STATUS_ENQUIRY);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("StatusEnquiryRequest", encrypt(rechargeStatusEnquiryRequestDTO), subHeaderDTO),
                createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("rechargeStatusEnquiry request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(rechargeStatusEnquiryRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisRechargeStatusEnquiryResponseDTO decryptedResponse = decrypt(response.getBody(),
                    "StatusEnquiryResponse", AxisRechargeStatusEnquiryResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("rechargeStatusEnquiry response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("rechargeStatusEnquiry response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisWalletStatusEnquiryResponseDTO walletStatusEnquiry(
            AxisWalletStatusEnquiryRequestDTO walletStatusEnquiryRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("WALLETSTATUS" + seqGenerator.nextId(), "STATUS-ENQUIRY");
        walletStatusEnquiryRequestDTO.setChannelId(CHANNEL_ID);
        walletStatusEnquiryRequestDTO.createCheckSum(CHECKSUM_KEY, subHeaderDTO);
        walletStatusEnquiryRequestDTO.setRefId(String.valueOf(seqGenerator.nextId()));
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.STATUS_ENQUIRY);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("StatusEnquiryRequest", encrypt(walletStatusEnquiryRequestDTO), subHeaderDTO),
                createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("walletStatusEnquiry request :: {} sent on Axis, sub header {}",
                    TransformUtil.toJson(walletStatusEnquiryRequestDTO), subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity, new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisWalletStatusEnquiryResponseDTO decryptedResponse = decrypt(response.getBody(), "StatusEnquiryResponse",
                    AxisWalletStatusEnquiryResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.info("walletStatusEnquiry response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("walletStatusEnquiry response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    @Override
    public AxisReplaceTagResponseDTO replaceTag(AxisReplaceTagRequestDTO replaceTagRequestDTO) {
        AxisSubHeaderDTO subHeaderDTO = getSubHeader("REPLACETAG" + seqGenerator.nextId(), "REPLACE-TAG");
        replaceTagRequestDTO.setPosNumber(POS_NUMBER);
        replaceTagRequestDTO.createCheckSum(CHECKSUM_KEY, subHeaderDTO);
        StringBuilder url = new StringBuilder(bankUrl);
        url.append(Constants.AXISBank.Urls.REPLACE_TAG);

        HttpEntity<Object> entity = new HttpEntity<>(
                getRequestBody("TagIssuanceRequest", encrypt(replaceTagRequestDTO), subHeaderDTO), createHeaders());
        Long requestStartTime = System.currentTimeMillis();

        try {
            LOG.info("replaceTag request :: {} sent on Axis, sub header {}", TransformUtil.toJson(replaceTagRequestDTO),
                    subHeaderDTO);
            ResponseEntity<Map<String, Map<String, Object>>> response = axisClient.exchange(url.toString(),
                    HttpMethod.POST, entity,
                    new ParameterizedTypeReference<Map<String, Map<String, Object>>>() {
                    });
            AxisReplaceTagResponseDTO decryptedResponse = decrypt(response.getBody(), "TagIssuanceResponse",
                    AxisReplaceTagResponseDTO.class);
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            if (decryptedResponse != null && ("500".equals(decryptedResponse.getStatusCode())
                    || "Exception occured".equals(decryptedResponse.getResponseMessage()))) {
                decryptedResponse.setResponseMessage("Bank error occured, Please try again.");
            }
            LOG.info("replaceTag response {} received after {} ms", decryptedResponse, responseTimeInMillis);
            return decryptedResponse;
        } catch (Exception e) {
            Sentry.getStoredClient().getContext().addTag(Constants.SENTRY_ACTIONABLE_TAG, "false");
            Long responseTimeInMillis = System.currentTimeMillis() - requestStartTime;
            LOG.error("replaceTag response exception {}, {} after {} ms", e.fillInStackTrace(), e,
                    responseTimeInMillis);
            Sentry.getStoredClient().getContext().removeTag(Constants.SENTRY_ACTIONABLE_TAG);
            throw new BankClientException(CustomErrorMessages.BANK_SERVERS_DOWN);
        }
    }

    private AxisSubHeaderDTO getSubHeader(String requestUuid, String serviceRequestId) {
        AxisSubHeaderDTO headerDTO = new AxisSubHeaderDTO();
        headerDTO.setRequestUUID(requestUuid);
        headerDTO.setServiceRequestId(serviceRequestId);
        headerDTO.setChannelId(CHANNEL_ID);
        headerDTO.setServiceRequestVersion("1.0");
        return headerDTO;
    }

    private Map<String, Object> getRequestBody(String rootKey, String requestBodyEncryptedValue,
            AxisSubHeaderDTO subHeaderDTO) {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> req = new HashMap<>();
        req.put("SubHeader", subHeaderDTO);
        req.put(rootKey + "BodyEncrypted", requestBodyEncryptedValue);
        root.put(rootKey, req);
        return root;
    }

    private <T> T decrypt(Map<String, Map<String, Object>> encrypted, String rootKey, Class<T> valueType) {
        if (encrypted == null) {
            return null;
        }
        Map<String, Object> object = encrypted.get(rootKey);
        String responseBody = EncryptionDecryptionUtil
                .decryptAxisResponse(((String) object.get(rootKey + "BodyEncrypted")), ENCRYPTION_DECRYPTION_KEY);
        return TransformUtil.fromJson(responseBody, valueType);
    }

    private String encrypt(Object o) {
        return EncryptionDecryptionUtil.encryptAxisRequest(TransformUtil.toJsonFieldVisibility(o),
                ENCRYPTION_DECRYPTION_KEY);
    }
}
