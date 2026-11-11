package sa.factory.hakeemvitalstelehe.data.repository

import sa.factory.hakeemvitalstelehe.domain.repository.HakeemVitalsRepository
import sa.factory.hakeemvitalstelehe.hardware.HardwareSensorDriver
import sa.factory.hakeemvitalstelehe.model.HakeemVitalsItem
import sa.factory.hakeemvitalstelehe.model.HakeemVitalsOrder
import sa.factory.hakeemvitalstelehe.model.HakeemVitalsTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class HakeemVitalsRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : HakeemVitalsRepository {

    private val _catalog = MutableStateFlow<List<HakeemVitalsItem>>(listOf(
        HakeemVitalsItem("1", "باقة الاستشارة الشاملة مع تقرير حيوي", "استشارة 45 دقيقة مع تحليل فوري لقراءات ضغط الدم والأكسجة عبر جهاز BLE المرفق", 350.0, "شامل", "استشارات تخصصية"),
        HakeemVitalsItem("2", "دورة إدارة الضغط النفسي", "كورس مسجل 8 ساعات مع تمارين تفاعلية ومتابعة أسبوعية عبر التطبيق", 499.0, "الأكثر طلباً", "دورات طبية مسجلة"),
        HakeemVitalsItem("3", "اشتراك شهري لمراقبة الضغط", "متابعة يومية عبر جهاز BLE مع تنبيهات طبية تلقائية وتحليل اتجاهات", 199.0, "جديد", "مراقبة العلامات الحيوية")
    ))
    override val catalogFlow: Flow<List<HakeemVitalsItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<HakeemVitalsItem>>(emptyList())
    override val activeCart: StateFlow<List<HakeemVitalsItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<HakeemVitalsTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            HakeemVitalsTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: HakeemVitalsItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: HakeemVitalsItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): HakeemVitalsOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = HakeemVitalsOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
