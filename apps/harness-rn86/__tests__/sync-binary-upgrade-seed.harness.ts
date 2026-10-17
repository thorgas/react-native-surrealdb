import { test } from 'react-native-harness';
import { runBinaryUpgradePhase } from '../../harness-shared/sync-binary-upgrade-fixture';
test('seeds a dedicated journal using the task11 binary', () =>
  runBinaryUpgradePhase('seed'));
