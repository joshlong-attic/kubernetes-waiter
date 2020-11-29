package com.joshlong.k8s;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@SpringBootApplication (proxyBeanMethods = false)
public class WaiterApplication {

	public static void main(String[] args) {
		SpringApplication.run(WaiterApplication.class, args);
	}

	@Bean
	Fabric8KubernetesClient waitingKubernetesClient() {
		return new Fabric8KubernetesClient();
	}

	@Bean
	ApplicationListener<ApplicationReadyEvent> ready(Fabric8KubernetesClient client) {
		return are -> client.waitForExternalIps(java.time.Duration.ofMinutes(5), java.time.Duration.ofSeconds(5), null, "configuration");
	}
}

/*

@Component
class KubernetesJavaClient implements ApplicationListener<ApplicationReadyEvent> {


	void begin(ApplicationReadyEvent are) throws IOException, ApiException {
		ApiClient client = Config.defaultClient();
		Configuration.setDefaultApiClient(client);

		CoreV1Api api = new CoreV1Api();
		V1PodList list = api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
		for (V1Pod item : list.getItems()) {
			System.out.println(Objects.requireNonNull(item.getMetadata()).getName());
		}
	}

	@Override
	@SneakyThrows
	public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
		begin(applicationReadyEvent);
	}
}

*/


@RequiredArgsConstructor
class Fabric8KubernetesClient {

	@SneakyThrows
	public void waitForExternalIps(java.time.Duration timeout, java.time.Duration delay, String namespace, String... serviceNames) {
		long timeout1 = timeout.getSeconds() * 1000;
		var start = System.currentTimeMillis();
		var max = start + timeout1;
		var message = "you must provide a list of services to poll";
		Assert.notNull(serviceNames, () -> message);
		Assert.state(serviceNames.length >= 1, () -> message);
		var matches = new ConcurrentHashMap<String, Boolean>();
		for (var svc : serviceNames) {
			matches.put(svc, false);
		}
		try (var client = new DefaultKubernetesClient()) {

			var ready = new AtomicBoolean();

			log("initial status: " + matches);
			while (!ready.get()) {

				Assert.state(System.currentTimeMillis() < max, () -> "we've exceeded the " + timeout1 + "ms timeout for the wait!");

				var serviceNamesList = matches
					.entrySet()
					.stream()
					.filter(e -> !e.getValue()).map(Map.Entry::getKey).collect(Collectors.toList());

				serviceNamesList
					.stream()
					.flatMap(svcName ->
						this.contextualizeToNamespace(client.pods(), namespace)
							.list()
							.getItems()
							.stream()
							.filter(pod -> pod.getMetadata().getName().contains(svcName))
							.map(p -> Collections.singletonMap(svcName, p))
							.map(p -> getServiceFor(client, namespace, p.keySet().iterator().next(), p.values().iterator().next()))
							.filter(Objects::nonNull)
							.filter(this::isExternallyAvailable)
					)
					.forEach(svc -> matches.put(svc.getMetadata().getName(), true));

				log(matches);
				ready.set(!matches.containsValue(false));
				if (!ready.get()) {
					Thread.sleep(delay.getSeconds() * 1000);
				}
			}

			log("final status: " + matches);
		}
		catch (KubernetesClientException ex) {
			System.err.println(ex);
		}
	}

	private void log(Object msg) {
		System.out.println(msg);
	}

	private Service getServiceFor(KubernetesClient kc, String ns, String servicePredicate, Pod pod) {
		var serviceName = pod.getMetadata().getName();
		return contextualizeToNamespace(kc.services(), ns)
			.list().getItems().stream().filter(svc -> svc.getMetadata().getName().contains(servicePredicate))
			.findAny()
			.orElse(null);
	}

	private <P, PL, DP, PR extends Resource<P, DP>> FilterWatchListMultiDeletable<P, PL, Boolean, Watch, Watcher<P>> contextualizeToNamespace(
		MixedOperation<P, PL, DP, PR> ops, String namespace) {
		return StringUtils.hasText(namespace) ?
			ops.inNamespace(namespace) :
			ops.inAnyNamespace();
	}

	private boolean isExternallyAvailable(Service service) {
		return service.getSpec().getExternalIPs().size() > 0 ||
			service.getStatus().getLoadBalancer().getIngress().size() > 0;
	}
}
