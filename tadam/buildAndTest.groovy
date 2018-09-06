import com.getbase.plumber.tadam.FlowID
import com.getbase.plumber.tadam.FlowRunner
import com.getbase.plumber.tadam.Team

FlowRunner.using(this).withOwnership(Team.BackendPlatform).run(FlowID.JavaGradleV1)
